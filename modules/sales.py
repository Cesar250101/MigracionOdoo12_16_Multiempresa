"""
modules/sales.py
Migración de ventas Odoo 12 -> Odoo 16.

Tablas manejadas:
  • procurement_group      - grupos de aprovisionamiento
  • crm_team               - equipos de ventas
  • sale_order             - pedidos de venta
  • sale_order_line        - líneas de pedido de venta
  • account_tax_sale_order_line_rel - M2M impuestos en líneas
"""

import logging

log = logging.getLogger(__name__)


class SalesMigrator:

    def __init__(self, base):
        """
        Args:
            base: instancia de BaseMigrator (acceso a src_conn, tgt_conn, id_map, etc.)
        """
        self.b = base

    # ──────────────────────────────────────────────
    # Migración principal
    # ──────────────────────────────────────────────

    def migrate_sales(self):
        """Migra sale.order y sale.order.line."""
        log.info("=== Migrando ventas ===")

        # Tarifas (product_pricelist): mapear antes de sale_order
        self._map_pricelist()

        # Fallback de tarifa: primera disponible en destino
        fallback_pricelist = self._get_fallback_pricelist()

        # Grupos de aprovisionamiento (sale_id es FK circular: se salta y actualiza después)
        self.b.migrate_table(
            'procurement_group',
            mapping_fields={'partner_id': 'res_partner'},
            skip_fields=['sale_id'],
        )

        # Equipos de ventas — alias_id es NOT NULL en Odoo 16 y requiere mail.alias;
        # se pre-mapean los equipos existentes por nombre y se omiten el resto.
        self._premap_crm_teams()

        # Pricelist_id: transform con fallback para evitar NOT NULL failure
        # Se excluye de mapping_fields para que field_transforms lo maneje solo
        pricelist_transform = (
            lambda v, _fb=fallback_pricelist: (
                self.b.id_map.get('product_pricelist', {}).get(v) or _fb
            )
        )

        # Pedidos de venta
        self.b.migrate_table(
            'sale_order',
            mapping_fields={
                'partner_id':           'res_partner',
                'partner_invoice_id':   'res_partner',
                'partner_shipping_id':  'res_partner',
                'journal_id':           'account_journal',
                'procurement_group_id': 'procurement_group',
                'team_id':              'crm_team',
                'warehouse_id':         'stock_warehouse',
                'fiscal_position_id':   'account_fiscal_position',
                'currency_id':          'res_currency',
                # pricelist_id se maneja por field_transforms (con fallback)
                'payment_term_id':      'account_payment_term',
            },
            skip_fields=['message_main_attachment_id'],
            field_transforms={
                'pricelist_id': pricelist_transform,
            },
        )

        # Líneas de pedido
        self.b.migrate_table(
            'sale_order_line',
            mapping_fields={
                'order_id':          'sale_order',
                'product_id':        'product_product',
                'order_partner_id':  'res_partner',
                'route_id':          'stock_route',
                'product_uom':       'uom_uom',
            },
        )

        # M2M: impuestos en líneas de pedido de venta
        self.b.migrate_m2m(
            'account_tax_sale_order_line_rel',
            'sale_order_line_id', 'account_tax_id',
            'sale_order_line', 'account_tax',
        )

        # Actualizar procurement_group.sale_id (FK circular resuelta aquí)
        self._update_procurement_group_sale()

        log.info("Migración de ventas completada.")

    # ──────────────────────────────────────────────
    # Helpers internos
    # ──────────────────────────────────────────────

    def _premap_crm_teams(self):
        """
        Pre-mapea crm_team origen -> destino por nombre.
        En Odoo 16, crm_team.alias_id es NOT NULL (FK a mail.alias), lo que impide
        insertar equipos de venta via SQL sin invocar el ORM.
        Se mapean los equipos existentes en destino y se omiten los demás.
        """
        log.info("Pre-mapeando equipos de ventas (crm_team) por nombre...")
        src_teams = self.b.fetch_src("SELECT id, name FROM crm_team")
        self.b.id_map.setdefault('crm_team', {})

        with self.b.tgt_conn.cursor() as cur:
            cur.execute("SELECT id, name FROM crm_team")
            tgt_map = {}
            for tid, tname in cur.fetchall():
                key = (tname if isinstance(tname, str) else
                       (tname.get('en_US', '') if isinstance(tname, dict) else str(tname))
                       ).lower().strip()
                tgt_map[key] = tid

        matched = 0
        for row in src_teams:
            src_name = row['name'] if isinstance(row['name'], str) else \
                       (row['name'].get('en_US', '') if isinstance(row['name'], dict)
                        else str(row['name']))
            key = src_name.lower().strip()
            if key in tgt_map:
                self.b.id_map['crm_team'][row['id']] = tgt_map[key]
                matched += 1
            else:
                log.debug("crm_team '%s' no encontrada en destino, se ignorará.", src_name)

        log.info("crm_team: %d/%d equipos mapeados.", matched, len(src_teams))

    def _map_pricelist(self):
        """
        Mapea product_pricelist del origen al destino por nombre + moneda.
        Si no existe en destino, lo inserta.
        """
        if not self.b.table_exists_in_src('product_pricelist'):
            return

        log.info("Mapeando/migrando tarifas (product_pricelist)...")
        src_rows = self.b.fetch_src(
            "SELECT id, name, currency_id, active FROM product_pricelist ORDER BY id"
        )
        self.b.id_map.setdefault('product_pricelist', {})

        tgt_cols = self.b.get_tgt_columns('product_pricelist')

        with self.b.tgt_conn.cursor() as cur:
            # Índice destino: nombre -> id (insensible a mayúsculas)
            cur.execute("SELECT id, name FROM product_pricelist")
            tgt_raw = cur.fetchall()

        # Normalizar nombres destino
        tgt_by_name = {}
        for tid, tname in tgt_raw:
            key = (tname if isinstance(tname, str) else
                   (tname.get('en_US', '') if isinstance(tname, dict) else str(tname))
                   ).lower().strip()
            tgt_by_name[key] = tid

        inserted_pl = 0
        with self.b.tgt_conn.cursor() as cur:
            for row in src_rows:
                old_id   = row['id']
                src_name = row['name'] if isinstance(row['name'], str) else \
                           (row['name'].get('en_US', '') if isinstance(row['name'], dict)
                            else str(row['name']))
                key = src_name.lower().strip()

                if key in tgt_by_name:
                    self.b.id_map['product_pricelist'][old_id] = tgt_by_name[key]
                    continue

                # Crear nueva tarifa en destino
                new_currency_id = self.b.id_map.get('res_currency', {}).get(
                    row.get('currency_id'),
                    next(iter(self.b.id_map.get('res_currency', {}).values()), None)
                )
                rec = {
                    'name': src_name,
                    'active': row.get('active', True),
                    'currency_id': new_currency_id,
                    'create_uid': 1,
                    'write_uid': 1,
                }
                rec = {k: v for k, v in rec.items() if k in tgt_cols}
                self.b._fill_not_null(rec, tgt_cols)
                cols_q = ', '.join(f'"{c}"' for c in rec)
                phs    = ', '.join(['%s'] * len(rec))
                try:
                    cur.execute(
                        f'INSERT INTO product_pricelist ({cols_q}) VALUES ({phs}) RETURNING id',
                        self.b.prepare_vals(rec, tgt_cols),
                    )
                    new_id = cur.fetchone()[0]
                    self.b.id_map['product_pricelist'][old_id] = new_id
                    tgt_by_name[key] = new_id
                    inserted_pl += 1
                except Exception as e:
                    self.b.tgt_conn.rollback()
                    log.error("product_pricelist old_id=%s: %s", old_id, e)

        log.info(
            "product_pricelist: %d mapeadas, %d creadas.",
            len(self.b.id_map['product_pricelist']) - inserted_pl,
            inserted_pl,
        )

    def _get_fallback_pricelist(self) -> int:
        """Retorna el id de la primera tarifa disponible en destino (fallback)."""
        with self.b.tgt_conn.cursor() as cur:
            cur.execute("SELECT id FROM product_pricelist WHERE active = true ORDER BY id LIMIT 1")
            row = cur.fetchone()
            return row[0] if row else None

    def _update_procurement_group_sale(self):
        """
        Actualiza procurement_group.sale_id después de migrar sale_order.
        Se saltó al inicio para evitar FK circular.
        """
        src_cols = self.b.get_src_columns('procurement_group')
        if 'sale_id' not in src_cols:
            return

        tgt_cols = self.b.get_tgt_columns('procurement_group')
        if 'sale_id' not in tgt_cols:
            return

        rows = self.b.fetch_src(
            "SELECT id, sale_id FROM procurement_group WHERE sale_id IS NOT NULL"
        )
        updated = 0
        with self.b.tgt_conn.cursor() as cur:
            for row in rows:
                tgt_grp  = self.b.id_map.get('procurement_group', {}).get(row['id'])
                tgt_sale = self.b.id_map.get('sale_order',         {}).get(row['sale_id'])
                if tgt_grp and tgt_sale:
                    try:
                        cur.execute(
                            "UPDATE procurement_group SET sale_id = %s WHERE id = %s",
                            (tgt_sale, tgt_grp),
                        )
                        updated += 1
                    except Exception:
                        self.b.tgt_conn.rollback()

        log.info("procurement_group.sale_id actualizados: %d", updated)
