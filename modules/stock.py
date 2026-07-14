"""
modules/stock.py
Migración de inventario/stock Odoo 12 -> Odoo 16.

Cambios críticos:
  • stock.production.lot  (table: stock_production_lot) ->
    stock.lot             (table: stock_lot)
  • stock_quant: en Odoo 12 múltiples quants por (product/loc/lot),
    en Odoo 16 se consolidan con reserved_quantity separado.
  • stock_move_line: compatible, pero lot_id FK apunta a stock_lot.
"""

import logging

log = logging.getLogger(__name__)


class StockMigrator:

    def __init__(self, base):
        self.b = base

    def migrate_locations(self):
        """Migra stock_location (recursivo por location_id)."""
        log.info("Migrando ubicaciones (stock_location)...")

        # Pre-mapear ubicaciones que ya existen en destino por complete_name
        # (evita UniqueViolation en barcode y permite mapear lot_stock_id en almacenes)
        self._premap_locations_by_name()

        self.b.migrate_table(
            'stock_location',
            is_recursive=True,
            recursive_field='location_id',
            # Omitir barcode: se auto-genera en Odoo 16 y puede causar duplicados
            skip_fields=['message_main_attachment_id', 'barcode'],
        )

    def _premap_locations_by_name(self):
        """Pre-mapea stock_location origen -> destino por complete_name."""
        log.info("Pre-mapeando ubicaciones existentes (complete_name)...")
        src_locs = self.b.fetch_src(
            "SELECT id, complete_name FROM stock_location ORDER BY id"
        )
        self.b.id_map.setdefault('stock_location', {})

        with self.b.tgt_conn.cursor() as cur:
            cur.execute("SELECT id, complete_name FROM stock_location")
            tgt_map = {}
            for tid, cname in cur.fetchall():
                if cname:
                    tgt_map[cname.strip().lower()] = tid

        matched = 0
        for row in src_locs:
            cname = (row['complete_name'] or '').strip().lower()
            if cname and cname in tgt_map:
                self.b.id_map['stock_location'][row['id']] = tgt_map[cname]
                matched += 1

        log.info("stock_location: %d ubicaciones pre-mapeadas por nombre.", matched)

    def migrate_warehouses(self):
        """
        Migra stock_warehouse. Los campos de rutas/reglas se omiten en este paso
        y se actualizan en post_migration_stock (evita dependencias circulares).
        Los picking_type fields se omiten aquí y se actualizan en post_migration_stock
        (después de migrate_picking_types) para evitar referencias inválidas.
        """
        log.info("Migrando almacenes (stock_warehouse)...")
        self.b.migrate_table(
            'stock_warehouse',
            mapping_fields={
                'partner_id': 'res_partner',
                'view_location_id': 'stock_location',
                'lot_stock_id': 'stock_location',
                'wh_input_stock_loc_id': 'stock_location',
                'wh_qc_stock_loc_id': 'stock_location',
                'wh_output_stock_loc_id': 'stock_location',
                'wh_pack_stock_loc_id': 'stock_location',
            },
            skip_fields=[
                # Rutas/reglas: se actualizan en post_migration_stock
                'mto_pull_id', 'manufacture_pull_id', 'manufacture_mto_pull_id',
                'pbm_mto_pull_id', 'sam_rule_id', 'buy_pull_id',
                'subcontracting_mto_pull_id', 'subcontracting_pull_id',
                'crossdock_route_id', 'reception_route_id', 'delivery_route_id',
                'pbm_route_id', 'subcontracting_route_id',
                # Picking types: se actualizan en post_migration_stock (después de migrate_picking_types)
                'in_type_id', 'out_type_id', 'pick_type_id', 'int_type_id',
                'pack_type_id', 'manu_type_id', 'pos_type_id',
                'pbm_type_id', 'sam_type_id',
                'return_type_id', 'subcontracting_type_id', 'subcontracting_resupply_type_id',
                'message_main_attachment_id',
            ],
        )

    def migrate_picking_types(self):
        """Migra stock_picking_type.
        return_picking_type_id se omite aquí (auto-referencia entre picking types)
        y se actualiza en post_migration_stock después de mapear todos los tipos.
        """
        log.info("Migrando tipos de operación (stock_picking_type)...")
        self.b.migrate_table(
            'stock_picking_type',
            mapping_fields={
                'warehouse_id': 'stock_warehouse',
                'default_location_src_id': 'stock_location',
                'default_location_dest_id': 'stock_location',
                'sequence_id': 'ir_sequence',
            },
            skip_fields=['return_picking_type_id'],
        )

    def _post_migrate_picking_type_returns(self):
        """Actualiza return_picking_type_id en stock_picking_type (auto-referencia)."""
        log.info("Post-migración: vinculando return_picking_type_id en stock_picking_type...")
        pt_map = self.b.id_map.get('stock_picking_type', {})
        if not pt_map:
            return

        src_rows = self.b.fetch_src(
            "SELECT id, return_picking_type_id FROM stock_picking_type "
            "WHERE return_picking_type_id IS NOT NULL"
        )
        updated = 0
        with self.b.tgt_conn.cursor() as cur:
            for row in src_rows:
                tgt_id = pt_map.get(row['id'])
                tgt_ret = pt_map.get(row['return_picking_type_id'])
                if tgt_id and tgt_ret:
                    try:
                        cur.execute(
                            "UPDATE stock_picking_type SET return_picking_type_id=%s WHERE id=%s",
                            (tgt_ret, tgt_id)
                        )
                        updated += 1
                    except Exception as e:
                        self.b.tgt_conn.rollback()
                        log.warning("return_picking_type_id src_id=%s: %s", row['id'], e)
        log.info("stock_picking_type: %d return_picking_type_id actualizados.", updated)

    def migrate_routes(self):
        """
        Migra rutas y reglas de abastecimiento.
        Odoo 12: tabla stock_location_route (modelo stock.location.route)
        Odoo 16: tabla stock_route          (modelo stock.route)
        """
        self._migrate_stock_routes()
        self._migrate_stock_rules()

    def _migrate_stock_routes(self):
        """
        Odoo 12: stock_location_route (modelo stock.location.route)
        Odoo 16: stock_route          (modelo stock.route, mismos campos salvo
                 el nombre de tabla/modelo).
        """
        log.info("Migrando rutas (stock_location_route -> stock_route)...")

        if not self.b.table_exists_in_src('stock_location_route'):
            log.warning("Tabla stock_location_route no encontrada en origen.")
            return
        if not self.b.table_exists_in_tgt('stock_route'):
            log.warning("Tabla stock_route no encontrada en destino.")
            return

        tgt_cols = self.b.get_tgt_columns('stock_route')
        routes = self.b.fetch_src("SELECT * FROM stock_location_route ORDER BY id")
        self.b.id_map.setdefault('stock_route', {})

        wh_map = self.b.id_map.get('stock_warehouse', {})

        inserted = 0
        mapped = 0
        with self.b.tgt_conn.cursor() as cur:
            for route in routes:
                old_id = route['id']
                rec = {
                    'name': route['name'],
                    'active': route.get('active', True),
                    'sequence': route.get('sequence'),
                    'product_selectable': route.get('product_selectable'),
                    'product_categ_selectable': route.get('product_categ_selectable'),
                    'warehouse_selectable': route.get('warehouse_selectable'),
                    'sale_selectable': route.get('sale_selectable'),
                    'supplied_wh_id': wh_map.get(route.get('supplied_wh_id')),
                    'supplier_wh_id': wh_map.get(route.get('supplier_wh_id')),
                    'company_id': self.b.map_company(route.get('company_id')),
                    'create_uid': 1,
                    'write_uid': 1,
                }
                self.b._fill_not_null(rec, tgt_cols)

                cols_q = ', '.join(f'"{c}"' for c in rec)
                placeholders = ', '.join(['%s'] * len(rec))
                try:
                    cur.execute(
                        f'INSERT INTO stock_route ({cols_q}) VALUES ({placeholders}) RETURNING id',
                        self.b.prepare_vals(rec, tgt_cols)
                    )
                    new_id = cur.fetchone()[0]
                    self.b.id_map['stock_route'][old_id] = new_id
                    inserted += 1
                except Exception as e:
                    self.b.tgt_conn.rollback()
                    try:
                        cur.execute(
                            "SELECT id FROM stock_route WHERE name->>'en_US'=%s AND company_id=%s",
                            (route['name'], rec['company_id'])
                        )
                        found = cur.fetchone()
                        if found:
                            self.b.id_map['stock_route'][old_id] = found[0]
                            mapped += 1
                        else:
                            log.warning("stock_route old_id=%s (%s): %s",
                                        old_id, route['name'], str(e).split('\n')[0])
                    except Exception:
                        self.b.tgt_conn.rollback()
                        log.warning("stock_route old_id=%s (%s): %s",
                                    old_id, route['name'], str(e).split('\n')[0])

        log.info("stock_route: insertados=%d mapeados(existentes)=%d", inserted, mapped)

    def _migrate_stock_rules(self):
        """
        Odoo 12: stock_rule.location_id     (destino de la regla)
        Odoo 16: stock_rule.location_dest_id (mismo campo, renombrado).
        Se migra manualmente porque migrate_table() no soporta renombrar
        columnas entre origen y destino.
        """
        log.info("Migrando reglas (stock_rule)...")

        if not self.b.table_exists_in_src('stock_rule'):
            log.warning("Tabla stock_rule no encontrada en origen.")
            return

        tgt_cols = self.b.get_tgt_columns('stock_rule')
        rules = self.b.fetch_src("SELECT * FROM stock_rule ORDER BY id")
        self.b.id_map.setdefault('stock_rule', {})

        loc_map = self.b.id_map.get('stock_location', {})
        route_map = self.b.id_map.get('stock_route', {})
        pt_map = self.b.id_map.get('stock_picking_type', {})
        wh_map = self.b.id_map.get('stock_warehouse', {})
        group_map = self.b.id_map.get('procurement_group', {})
        partner_map = self.b.id_map.get('res_partner', {})

        inserted = 0
        skipped = 0
        with self.b.tgt_conn.cursor() as cur:
            for rule in rules:
                old_id = rule['id']

                new_location_dest_id = loc_map.get(rule.get('location_id'))
                new_route_id = route_map.get(rule.get('route_id'))
                new_picking_type_id = pt_map.get(rule.get('picking_type_id'))

                if not new_location_dest_id or not new_route_id or not new_picking_type_id:
                    log.warning(
                        "Saltando stock_rule old_id=%s (%s): FK requerida sin mapear "
                        "(location_dest_id=%s, route_id=%s, picking_type_id=%s)",
                        old_id, rule.get('name'),
                        new_location_dest_id, new_route_id, new_picking_type_id
                    )
                    skipped += 1
                    continue

                rec = {
                    'name': rule['name'],
                    'active': rule.get('active', True),
                    'group_propagation_option': rule.get('group_propagation_option'),
                    'group_id': group_map.get(rule.get('group_id')),
                    'action': rule.get('action'),
                    'sequence': rule.get('sequence'),
                    'company_id': self.b.map_company(rule.get('company_id')),
                    'location_dest_id': new_location_dest_id,
                    'location_src_id': loc_map.get(rule.get('location_src_id')),
                    'route_id': new_route_id,
                    'procure_method': rule.get('procure_method'),
                    'route_sequence': rule.get('route_sequence'),
                    'picking_type_id': new_picking_type_id,
                    'delay': rule.get('delay'),
                    'partner_address_id': partner_map.get(rule.get('partner_address_id')),
                    'warehouse_id': wh_map.get(rule.get('warehouse_id')),
                    'propagate_warehouse_id': wh_map.get(rule.get('propagate_warehouse_id')),
                    'auto': rule.get('auto'),
                    'create_uid': 1,
                    'write_uid': 1,
                }
                self.b._fill_not_null(rec, tgt_cols)

                cols_q = ', '.join(f'"{c}"' for c in rec)
                placeholders = ', '.join(['%s'] * len(rec))
                try:
                    cur.execute(
                        f'INSERT INTO stock_rule ({cols_q}) VALUES ({placeholders}) RETURNING id',
                        self.b.prepare_vals(rec, tgt_cols)
                    )
                    new_id = cur.fetchone()[0]
                    self.b.id_map['stock_rule'][old_id] = new_id
                    inserted += 1
                except Exception as e:
                    self.b.tgt_conn.rollback()
                    log.warning("stock_rule old_id=%s (%s): %s",
                                old_id, rule.get('name'), str(e).split('\n')[0])
                    skipped += 1

        log.info("stock_rule: insertados=%d saltados=%d", inserted, skipped)

    def migrate_lots(self):
        """
        Migra lotes/números de serie.
        Odoo 12: tabla stock_production_lot (modelo stock.production.lot)
        Odoo 16: tabla stock_lot           (modelo stock.lot)
        """
        log.info("Migrando lotes/series (stock_production_lot -> stock_lot)...")

        if not self.b.table_exists_in_src('stock_production_lot'):
            log.warning("Tabla stock_production_lot no encontrada en origen.")
            return
        if not self.b.table_exists_in_tgt('stock_lot'):
            log.warning("Tabla stock_lot no encontrada en destino.")
            return

        src_cols = self.b.get_src_columns('stock_production_lot')
        tgt_cols = self.b.get_tgt_columns('stock_lot')

        lots = self.b.fetch_src("SELECT * FROM stock_production_lot ORDER BY id")
        self.b.id_map.setdefault('stock_lot', {})

        inserted = 0
        with self.b.tgt_conn.cursor() as cur:
            for lot in lots:
                old_id = lot['id']
                new_company_id = self.b.map_company(lot.get('company_id'))
                new_product_id = self.b.id_map.get('product_product', {}).get(lot['product_id'])

                rec = {
                    'name': lot['name'],
                    'product_id': new_product_id,
                    'company_id': new_company_id,
                    'create_uid': 1,
                    'write_uid': 1,
                }

                # Campos opcionales compatibles
                for fld in ('ref', 'expiration_date', 'use_date', 'removal_date',
                            'alert_date', 'note'):
                    if fld in src_cols and fld in tgt_cols:
                        rec[fld] = lot.get(fld)

                # product_uom_id -> product_qty en Odoo 12 se ignora aquí (va en quants)

                cols_q = ', '.join(f'"{c}"' for c in rec)
                placeholders = ', '.join(['%s'] * len(rec))
                try:
                    cur.execute(
                        f'INSERT INTO stock_lot ({cols_q}) VALUES ({placeholders}) RETURNING id',
                        self.b.prepare_vals(rec, tgt_cols)
                    )
                    new_id = cur.fetchone()[0]
                    self.b.id_map['stock_lot'][old_id] = new_id
                    inserted += 1
                except Exception as e:
                    self.b.tgt_conn.rollback()
                    # Intentar recuperar por name+product_id+company_id
                    try:
                        cur.execute(
                            'SELECT id FROM stock_lot WHERE name=%s AND product_id=%s AND company_id=%s',
                            (lot['name'], new_product_id, new_company_id)
                        )
                        res = cur.fetchone()
                        if res:
                            self.b.id_map['stock_lot'][old_id] = res[0]
                        else:
                            log.error("stock_lot old_id=%s: %s", old_id, e)
                    except Exception:
                        self.b.tgt_conn.rollback()

        log.info("stock_lot: %d lotes migrados.", inserted)

    def migrate_pickings(self):
        """Migra stock_picking (entregas, recepciones, transferencias)."""
        log.info("Migrando albaranes (stock_picking)...")

        # user_id (responsable): los usuarios no se migran; se conserva el mismo
        # uid solo si existe en destino, si no queda NULL (evita FK violation).
        self.b.preload_id_map('res_users')

        # currency_id no existe en Odoo 12 stock_picking (campo relacionado no almacenado).
        # En destinos donde el módulo que agrega la columna está instalado, Odoo 16 la
        # requiere NOT NULL; inyectamos el default solo si la columna existe realmente.
        extra_defaults = {}
        if 'currency_id' in self.b.get_tgt_columns('stock_picking'):
            with self.b.tgt_conn.cursor() as cur:
                cur.execute("SELECT id FROM res_currency WHERE name='CLP' LIMIT 1")
                row = cur.fetchone()
                extra_defaults['currency_id'] = row[0] if row else None

        self.b.migrate_table(
            'stock_picking',
            mapping_fields={
                'partner_id': 'res_partner',
                'location_id': 'stock_location',
                'location_dest_id': 'stock_location',
                'picking_type_id': 'stock_picking_type',
                'group_id': 'procurement_group',
                'sale_id': 'sale_order',
                'purchase_id': 'purchase_order',
                'backorder_id': 'stock_picking',
                'user_id': 'res_users',
            },
            skip_fields=['message_main_attachment_id'],
            extra_defaults=extra_defaults,
        )

    def migrate_moves(self):
        """Migra stock_move."""
        log.info("Migrando movimientos de stock (stock_move)...")
        self.b.migrate_table(
            'stock_move',
            mapping_fields={
                'product_id': 'product_product',
                'location_id': 'stock_location',
                'location_dest_id': 'stock_location',
                'partner_id': 'res_partner',
                'picking_id': 'stock_picking',
                'group_id': 'procurement_group',
                'picking_type_id': 'stock_picking_type',
                'origin_returned_move_id': 'stock_move',
                'warehouse_id': 'stock_warehouse',
                'sale_line_id': 'sale_order_line',
                'purchase_line_id': 'purchase_order_line',
                'rule_id': 'stock_rule',
            },
            skip_fields=[
                'move_line_ids', 'production_id', 'raw_material_production_id',
                'created_production_id', 'unbuild_id', 'consume_unbuild_id',
                'operation_id', 'workorder_id', 'bom_line_id', 'byproduct_id',
                'order_finished_lot_id', 'message_main_attachment_id',
                'repair_id',  # FK a repair_order; se vincula en update_moves_repair_id() tras repair.migrate_all()
            ],
        )

    def migrate_move_lines(self):
        """
        Migra stock_move_line.
        lot_id apunta a stock_lot (antes stock_production_lot).
        """
        log.info("Migrando líneas de movimiento (stock_move_line)...")

        if not self.b.table_exists_in_src('stock_move_line'):
            return

        src_cols = self.b.get_src_columns('stock_move_line')
        tgt_cols = self.b.get_tgt_columns('stock_move_line')

        lines = self.b.fetch_src("SELECT * FROM stock_move_line ORDER BY id")
        self.b.id_map.setdefault('stock_move_line', {})

        skip = {'id'}
        cols_copy = [c for c in tgt_cols if c != 'id' and c in src_cols and c not in skip]

        inserted = 0
        with self.b.tgt_conn.cursor() as cur:
            for line in lines:
                old_id = line['id']
                rec = {c: line[c] for c in cols_copy}

                rec['company_id'] = self.b.map_company(line.get('company_id'))
                rec['create_uid'] = 1
                rec['write_uid'] = 1

                for fk, ref in [
                    ('move_id', 'stock_move'),
                    ('picking_id', 'stock_picking'),
                    ('product_id', 'product_product'),
                    ('product_uom_id', 'uom_uom'),
                    ('location_id', 'stock_location'),
                    ('location_dest_id', 'stock_location'),
                    ('lot_id', 'stock_lot'),         # CRÍTICO: stock_lot en v16
                    ('result_package_id', 'stock_quant_package'),
                    ('package_id', 'stock_quant_package'),
                    ('owner_id', 'res_partner'),
                ]:
                    if fk in rec and rec[fk]:
                        rec[fk] = self.b.id_map.get(ref, {}).get(rec[fk])

                for f in list(rec.keys()):
                    if f.endswith('_id') and rec[f] == 0:
                        rec[f] = None

                # Fallback para product_uom_id (requerido en Odoo 16)
                if not rec.get('product_uom_id') and rec.get('product_id'):
                    # Intentar obtener uom_id del producto destino directamente
                    try:
                        with self.b.tgt_conn.cursor() as cur2:
                            cur2.execute("SELECT uom_id FROM product_template pt JOIN product_product pp ON pp.product_tmpl_id = pt.id WHERE pp.id = %s", (rec['product_id'],))
                            res = cur2.fetchone()
                            if res:
                                rec['product_uom_id'] = res[0]
                    except Exception as e:
                        self.b.tgt_conn.rollback()

                self.b._fill_not_null(rec, tgt_cols)

                cols_q = ', '.join(f'"{c}"' for c in rec)
                placeholders = ', '.join(['%s'] * len(rec))
                try:
                    cur.execute(
                        f'INSERT INTO stock_move_line ({cols_q}) VALUES ({placeholders}) RETURNING id',
                        self.b.prepare_vals(rec, tgt_cols)
                    )
                    new_id = cur.fetchone()[0]
                    self.b.id_map['stock_move_line'][old_id] = new_id
                    inserted += 1
                except Exception as e:
                    self.b.tgt_conn.rollback()
                    log.error("stock_move_line old_id=%s: %s", old_id, e)

        log.info("stock_move_line: %d líneas migradas.", inserted)

    def migrate_quants(self):
        """
        Migra stock_quant consolidando registros de Odoo 12.

        En Odoo 12, múltiples quants pueden existir para la misma
        (product, location, lot) con reservation_id en movimientos.
        En Odoo 16, se consolida en un solo registro con:
          - quantity = suma de qty donde reservation_id IS NULL
          - reserved_quantity = suma de qty donde reservation_id IS NOT NULL
        """
        log.info("Migrando existencias en stock (stock_quant consolidado)...")

        if not self.b.table_exists_in_src('stock_quant'):
            return

        src_cols = self.b.get_src_columns('stock_quant')
        has_reservation = 'reservation_id' in src_cols

        if has_reservation:
            quants = self.b.fetch_src("""
                SELECT
                    product_id,
                    location_id,
                    lot_id,
                    package_id,
                    owner_id,
                    company_id,
                    SUM(CASE WHEN reservation_id IS NULL THEN qty ELSE 0 END) AS quantity,
                    SUM(CASE WHEN reservation_id IS NOT NULL THEN qty ELSE 0 END) AS reserved_quantity,
                    MAX(in_date) AS in_date
                FROM stock_quant
                WHERE location_id IN (
                    SELECT id FROM stock_location WHERE usage = 'internal'
                )
                GROUP BY product_id, location_id, lot_id, package_id, owner_id, company_id
                HAVING SUM(qty) <> 0
            """)
        else:
            # Odoo 12 sin reservation_id (versiones con quants consolidados)
            quants = self.b.fetch_src("""
                SELECT product_id, location_id, lot_id, package_id, owner_id, company_id,
                       quantity, reserved_quantity, in_date
                FROM stock_quant
                WHERE location_id IN (
                    SELECT id FROM stock_location WHERE usage = 'internal'
                )
            """)

        inserted = 0
        tgt_quant_cols = self.b.get_tgt_columns('stock_quant')
        with self.b.tgt_conn.cursor() as cur:
            for q in quants:
                new_product = self.b.id_map.get('product_product', {}).get(q['product_id'])
                new_location = self.b.id_map.get('stock_location', {}).get(q['location_id'])
                if not new_product or not new_location:
                    continue

                rec = {
                    'product_id': new_product,
                    'location_id': new_location,
                    'lot_id': self.b.id_map.get('stock_lot', {}).get(q['lot_id']),
                    'package_id': self.b.id_map.get('stock_quant_package', {}).get(q['package_id']),
                    'owner_id': self.b.id_map.get('res_partner', {}).get(q['owner_id']),
                    'company_id': self.b.map_company(q['company_id']),
                    'quantity': float(q['quantity'] or 0),
                    'reserved_quantity': float(q.get('reserved_quantity') or 0),
                    'in_date': q.get('in_date'),
                    'create_uid': 1,
                    'write_uid': 1,
                }

                # Limpiar FK=0
                for f in list(rec.keys()):
                    if f.endswith('_id') and rec[f] == 0:
                        rec[f] = None

                cols_q = ', '.join(f'"{c}"' for c in rec)
                placeholders = ', '.join(['%s'] * len(rec))
                try:
                    cur.execute(
                        f'INSERT INTO stock_quant ({cols_q}) VALUES ({placeholders})',
                        self.b.prepare_vals(rec, tgt_quant_cols)
                    )
                    inserted += 1
                except Exception as e:
                    self.b.tgt_conn.rollback()
                    log.warning("stock_quant insert error: %s", e)

        log.info("stock_quant: %d quants consolidados migrados.", inserted)

    def post_migration_stock(self):
        """
        Actualiza referencias circulares en stock_warehouse después de migrar
        rutas y reglas.
        """
        log.info("Post-migración: vinculando rutas/reglas a almacenes...")

        sw_map = self.b.id_map.get('stock_warehouse', {})
        if not sw_map:
            return

        src_cols = self.b.get_src_columns('stock_warehouse')
        route_rule_fields = [
            ('mto_pull_id', 'stock_rule'),
            ('manufacture_pull_id', 'stock_rule'),
            ('manufacture_mto_pull_id', 'stock_rule'),
            ('pbm_mto_pull_id', 'stock_rule'),
            ('sam_rule_id', 'stock_rule'),
            ('buy_pull_id', 'stock_rule'),
            ('subcontracting_mto_pull_id', 'stock_rule'),
            ('subcontracting_pull_id', 'stock_rule'),
            ('crossdock_route_id', 'stock_route'),
            ('reception_route_id', 'stock_route'),
            ('delivery_route_id', 'stock_route'),
            ('pbm_route_id', 'stock_route'),
            ('subcontracting_route_id', 'stock_route'),
            # Picking types: ahora se mapean aquí después de migrate_picking_types
            ('in_type_id', 'stock_picking_type'),
            ('out_type_id', 'stock_picking_type'),
            ('pick_type_id', 'stock_picking_type'),
            ('int_type_id', 'stock_picking_type'),
            ('pack_type_id', 'stock_picking_type'),
            ('manu_type_id', 'stock_picking_type'),
            ('pos_type_id', 'stock_picking_type'),
            ('pbm_type_id', 'stock_picking_type'),
            ('sam_type_id', 'stock_picking_type'),
            ('return_type_id', 'stock_picking_type'),
            ('subcontracting_type_id', 'stock_picking_type'),
            ('subcontracting_resupply_type_id', 'stock_picking_type'),
        ]

        active_fields = [(f, t) for f, t in route_rule_fields if f in src_cols]
        if not active_fields:
            return

        field_names = [f for f, _ in active_fields]
        src_rows = self.b.fetch_src(
            f"SELECT id, {', '.join(field_names)} FROM stock_warehouse"
        )

        updated = 0
        with self.b.tgt_conn.cursor() as cur:
            for row in src_rows:
                tgt_sw_id = sw_map.get(row['id'])
                if not tgt_sw_id:
                    continue
                update_vals = {}
                for field, ref_table in active_fields:
                    old_ref = row.get(field)
                    if old_ref:
                        new_ref = self.b.id_map.get(ref_table, {}).get(old_ref)
                        if new_ref:
                            update_vals[field] = new_ref
                if update_vals:
                    set_clause = ', '.join(f'"{f}"=%s' for f in update_vals)
                    try:
                        cur.execute(
                            f'UPDATE stock_warehouse SET {set_clause} WHERE id=%s',
                            list(update_vals.values()) + [tgt_sw_id]
                        )
                        updated += 1
                    except Exception as e:
                        self.b.tgt_conn.rollback()
                        log.warning("stock_warehouse update id=%s: %s", tgt_sw_id, e)

        log.info("stock_warehouse post-migración: %d almacenes actualizados.", updated)

        # Actualizar return_picking_type_id (auto-referencia entre picking types)
        self._post_migrate_picking_type_returns()

    def update_moves_repair_id(self):
        """Segundo paso: vincula stock_move.repair_id tras migrate_all() de reparaciones."""
        repair_map = self.b.id_map.get('repair_order', {})
        if not repair_map:
            log.info("stock_move.repair_id: sin mapa de reparaciones, saltando.")
            return
        move_map = self.b.id_map.get('stock_move', {})
        src_moves = self.b.fetch_src(
            "SELECT id, repair_id FROM stock_move WHERE repair_id IS NOT NULL"
        )
        updated = 0
        with self.b.tgt_conn.cursor() as cur:
            for row in src_moves:
                new_repair_id = repair_map.get(row['repair_id'])
                new_move_id = move_map.get(row['id'])
                if new_repair_id and new_move_id:
                    try:
                        cur.execute(
                            "UPDATE stock_move SET repair_id=%s WHERE id=%s",
                            (new_repair_id, new_move_id)
                        )
                        updated += 1
                    except Exception as e:
                        self.b.tgt_conn.rollback()
                        log.warning("stock_move repair_id update old_id=%s: %s", row['id'], e)
            self.b.tgt_conn.commit()
        log.info("stock_move.repair_id: %d movimientos vinculados a reparaciones.", updated)
