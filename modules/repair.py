"""
modules/repair.py
Migración del módulo de reparaciones (repair) Odoo 12 -> Odoo 16.

Tablas migradas (en orden de dependencia):
  repair_order     -> repair_order
  repair_line      -> repair_line      (piezas/materiales usados)
  repair_fee       -> repair_fee       (mano de obra / servicios)
  repair_operations -> repair_operations (en Odoo 16 se llama igual)

Cambios relevantes Odoo 12 -> 16:
  • repair_order.state:
      '2binvoiced' -> 'done'  (ya no existe estado de pre-facturación)
      'under_repair' -> 'under_repair'
      'draft' -> 'draft'
      'cancel' -> 'cancel'
      'done' -> 'done'
      'ready' -> 'under_repair'
  • invoice_method ya no existe en Odoo 16 (ignorar).
  • fees_lines (repair_fee) sigue existiendo en Odoo 16.
  • operations (repair_line) sigue existiendo en Odoo 16.
  • stock_move FK en repair_line/repair_fee: se mapea a stock_move.
  • lot_id -> lot_id (stock_lot en v16 = stock_production_lot en v12).
"""

import logging

log = logging.getLogger(__name__)


# Mapeo de estados repair_order Odoo 12 -> Odoo 16
REPAIR_STATE_MAP = {
    'draft':        'draft',
    'confirmed':    'confirmed',
    'under_repair': 'under_repair',
    'ready':        'under_repair',   # 'ready' desaparece en v16
    '2binvoiced':   'done',           # 'to be invoiced' -> done
    'invoice_except': 'done',
    'done':         'done',
    'cancel':       'cancel',
}


class RepairMigrator:

    def __init__(self, base):
        self.b = base

    # ------------------------------------------------------------------
    # Punto de entrada: migrar todo el módulo de reparaciones
    # ------------------------------------------------------------------

    def migrate_all(self):
        """Migra todas las tablas del módulo repair en orden correcto."""
        log.info("=== Migrando módulo de Reparaciones (repair) ===")

        if not self.b.table_exists_in_src('repair_order'):
            log.warning("Tabla repair_order no encontrada en origen. Saltando módulo repair.")
            return

        if not self.b.table_exists_in_tgt('repair_order'):
            log.warning("Tabla repair_order no encontrada en destino. "
                        "Asegúrese de que el módulo 'repair' esté instalado en Odoo 16.")
            return

        self.migrate_orders()
        self.migrate_lines()
        self.migrate_fees()

        log.info("=== Módulo repair migrado. ===")

    # ------------------------------------------------------------------
    # repair.order
    # ------------------------------------------------------------------

    def migrate_orders(self):
        """
        Migra repair_order.
        Campos clave:
          product_id    -> product_product
          product_uom   -> uom_uom
          partner_id    -> res_partner
          address_id    -> res_partner
          invoice_id    -> account_move  (factura Odoo 12)
          picking_id    -> stock_picking
          lot_id        -> stock_lot     (stock_production_lot en v12)
          location_id   -> stock_location
          location_dest_id -> stock_location
          company_id    -> company (mapeado)
          state         -> transformado vía REPAIR_STATE_MAP
        """
        log.info("Migrando órdenes de reparación (repair_order)...")

        if not self.b.table_exists_in_src('repair_order'):
            return

        src_cols = self.b.get_src_columns('repair_order')
        tgt_cols = self.b.get_tgt_columns('repair_order')

        # Campos que no existen en Odoo 16 o que se renombran
        SKIP = {
            'id',
            'message_main_attachment_id',
            'fees_lines',            # Campo inverso virtual
            'operations',            # Campo inverso virtual
            'lot_id',                # Se mapea a 'patente' (nombre del lote), no como FK
            'name',                  # Se copia a repair_order_source_name
        }

        # Pre-cargar nombres de lotes/series para mapear lot_id -> patente
        lot_names = {}
        if self.b.table_exists_in_src('stock_production_lot'):
            for r in self.b.fetch_src("SELECT id, name FROM stock_production_lot"):
                lot_names[r['id']] = r['name']

        # Pre-cargar hr.employee: mapeo directo por ID (empleados ya configurados en destino)
        self.b.preload_id_map('hr_employee')

        rows = self.b.fetch_src("SELECT * FROM repair_order ORDER BY id")
        self.b.id_map.setdefault('repair_order', {})

        inserted = 0
        with self.b.tgt_conn.cursor() as cur:
            for row in rows:
                old_id = row['id']

                # Construir rec con campos comunes
                rec = {}
                for col in src_cols:
                    if col in SKIP or col not in tgt_cols:
                        continue
                    rec[col] = row[col]

                # ── lot_id → patente (nombre del lote = patente/placa del vehículo) ──
                lot_id = row.get('lot_id')
                if lot_id and 'patente' in tgt_cols:
                    rec['patente'] = lot_names.get(lot_id)

                # ── name → repair_order_source_name ──────────────────
                if 'repair_order_source_name' in tgt_cols:
                    rec['repair_order_source_name'] = row.get('name')
                # Preservar también en el campo name para mantener la referencia visible
                if 'name' in tgt_cols:
                    rec['name'] = row.get('name')

                # ── FKs ──────────────────────────────────────────────
                rec['company_id'] = self.b.map_company(row.get('company_id'))

                _fk_map = [
                    ('product_id',          'product_product'),
                    ('product_uom',         'uom_uom'),
                    ('partner_id',          'res_partner'),
                    ('address_id',          'res_partner'),
                    ('partner_invoice_id',  'res_partner'),
                    ('invoice_id',          'account_move'),
                    ('move_id',             'stock_move'),
                    ('picking_id',          'stock_picking'),
                    ('picking_type_id',     'stock_picking_type'),
                    ('location_id',         'stock_location'),
                    ('location_dest_id',    'stock_location'),
                    ('pricelist_id',        'product_pricelist'),
                    ('currency_id',         'res_currency'),
                    ('employee_id',         'hr_employee'),
                    ('user_id',             None),   # mantener uid si coincide o poner None
                ]
                for fk, ref in _fk_map:
                    if fk in rec:
                        val = row.get(fk)
                        if val and ref:
                            rec[fk] = self.b.id_map.get(ref, {}).get(val)
                        elif not ref:
                            rec[fk] = val  # uid: dejar tal cual (suele coincidir admin=1)

                # ── Estado ───────────────────────────────────────────
                if 'state' in tgt_cols:
                    rec['state'] = REPAIR_STATE_MAP.get(
                        row.get('state', 'draft'), 'draft'
                    )

                # ── Limpiar FK=0 ─────────────────────────────────────
                for f in list(rec.keys()):
                    if f.endswith('_id') and rec[f] == 0:
                        rec[f] = None

                rec['create_uid'] = 1
                rec['write_uid'] = 1

                self.b._fill_not_null(rec, tgt_cols)

                cols_q = ', '.join(f'"{c}"' for c in rec)
                placeholders = ', '.join(['%s'] * len(rec))
                try:
                    cur.execute(
                        f'INSERT INTO repair_order ({cols_q}) VALUES ({placeholders}) RETURNING id',
                        self.b.prepare_vals(rec, tgt_cols),
                    )
                    new_id = cur.fetchone()[0]
                    self.b.id_map['repair_order'][old_id] = new_id
                    inserted += 1
                except Exception as e:
                    self.b.tgt_conn.rollback()
                    log.error("repair_order old_id=%s: %s", old_id, e)

        log.info("repair_order: %d órdenes migradas.", inserted)

    # ------------------------------------------------------------------
    # repair.line  (materiales / piezas usadas)
    # ------------------------------------------------------------------

    def migrate_lines(self):
        """
        Migra repair_line (piezas / materiales consumidos).
        En Odoo 16 se llama igual: repair_line.
        """
        log.info("Migrando líneas de materiales de reparación (repair_line)...")

        if not self.b.table_exists_in_src('repair_line'):
            log.info("  repair_line no existe en origen, saltando.")
            return
        if not self.b.table_exists_in_tgt('repair_line'):
            log.warning("  repair_line no existe en destino, saltando.")
            return

        src_cols = self.b.get_src_columns('repair_line')
        tgt_cols = self.b.get_tgt_columns('repair_line')

        SKIP = {'id', 'message_main_attachment_id'}

        rows = self.b.fetch_src("SELECT * FROM repair_line ORDER BY id")
        self.b.id_map.setdefault('repair_line', {})

        inserted = 0
        with self.b.tgt_conn.cursor() as cur:
            for row in rows:
                old_id = row['id']

                # Verificar que el repair_order padre fue migrado
                new_repair_id = self.b.id_map['repair_order'].get(row.get('repair_id'))
                if not new_repair_id:
                    log.debug("repair_line old_id=%s: repair_order padre no mapeado, saltando.", old_id)
                    continue

                rec = {}
                for col in src_cols:
                    if col in SKIP or col not in tgt_cols:
                        continue
                    rec[col] = row[col]

                rec['repair_id'] = new_repair_id
                rec['company_id'] = self.b.map_company(row.get('company_id'))

                _fk_map = [
                    ('product_id',         'product_product'),
                    ('product_uom',        'uom_uom'),
                    ('lot_id',             'stock_lot'),
                    ('move_id',            'stock_move'),
                    ('location_id',        'stock_location'),
                    ('location_dest_id',   'stock_location'),
                    ('invoice_line_id',    'account_move_line'),
                ]
                for fk, ref in _fk_map:
                    if fk in rec:
                        val = row.get(fk)
                        rec[fk] = self.b.id_map.get(ref, {}).get(val) if val else None

                # tax_id M2M: se maneja post-insert por separado (migrate_lines_taxes)
                for f in list(rec.keys()):
                    if f.endswith('_id') and rec[f] == 0:
                        rec[f] = None

                rec['create_uid'] = 1
                rec['write_uid'] = 1
                self.b._fill_not_null(rec, tgt_cols)

                cols_q = ', '.join(f'"{c}"' for c in rec)
                placeholders = ', '.join(['%s'] * len(rec))
                try:
                    cur.execute(
                        f'INSERT INTO repair_line ({cols_q}) VALUES ({placeholders}) RETURNING id',
                        self.b.prepare_vals(rec, tgt_cols),
                    )
                    new_id = cur.fetchone()[0]
                    self.b.id_map['repair_line'][old_id] = new_id
                    inserted += 1
                except Exception as e:
                    self.b.tgt_conn.rollback()
                    log.error("repair_line old_id=%s: %s", old_id, e)

        log.info("repair_line: %d líneas de materiales migradas.", inserted)

        # M2M impuestos en líneas de material
        self._migrate_repair_line_taxes()

    def _migrate_repair_line_taxes(self):
        """Migra la relación M2M repair_line <-> account_tax."""
        # Nombre de la tabla M2M puede variar
        for m2m_table in ('repair_line_account_tax_rel', 'repair_operations_account_tax',
                          'account_tax_repair_line_rel'):
            if self.b.table_exists_in_src(m2m_table) and self.b.table_exists_in_tgt(m2m_table):
                # Detectar nombres de columnas
                src_cols = list(self.b.get_src_columns(m2m_table))
                repair_col = next((c for c in src_cols if 'repair' in c and 'line' in c), None)
                tax_col = next((c for c in src_cols if 'tax' in c), None)
                if repair_col and tax_col:
                    self.b.migrate_m2m(
                        m2m_table, repair_col, tax_col,
                        'repair_line', 'account_tax'
                    )
                return

    # ------------------------------------------------------------------
    # repair.fee  (honorarios / mano de obra)
    # ------------------------------------------------------------------

    def migrate_fees(self):
        """
        Migra repair_fee (líneas de mano de obra / servicios).
        En Odoo 16 se llama igual: repair_fee.
        """
        log.info("Migrando líneas de honorarios de reparación (repair_fee)...")

        if not self.b.table_exists_in_src('repair_fee'):
            log.info("  repair_fee no existe en origen, saltando.")
            return
        if not self.b.table_exists_in_tgt('repair_fee'):
            log.warning("  repair_fee no existe en destino, saltando.")
            return

        src_cols = self.b.get_src_columns('repair_fee')
        tgt_cols = self.b.get_tgt_columns('repair_fee')

        SKIP = {'id', 'message_main_attachment_id'}

        rows = self.b.fetch_src("SELECT * FROM repair_fee ORDER BY id")
        self.b.id_map.setdefault('repair_fee', {})

        inserted = 0
        with self.b.tgt_conn.cursor() as cur:
            for row in rows:
                old_id = row['id']

                new_repair_id = self.b.id_map['repair_order'].get(row.get('repair_id'))
                if not new_repair_id:
                    log.debug("repair_fee old_id=%s: repair_order padre no mapeado, saltando.", old_id)
                    continue

                rec = {}
                for col in src_cols:
                    if col in SKIP or col not in tgt_cols:
                        continue
                    rec[col] = row[col]

                rec['repair_id'] = new_repair_id
                rec['company_id'] = self.b.map_company(row.get('company_id'))

                _fk_map = [
                    ('product_id',       'product_product'),
                    ('product_uom',      'uom_uom'),
                    ('invoice_line_id',  'account_move_line'),
                    ('move_id',          'stock_move'),
                ]
                for fk, ref in _fk_map:
                    if fk in rec:
                        val = row.get(fk)
                        rec[fk] = self.b.id_map.get(ref, {}).get(val) if val else None

                for f in list(rec.keys()):
                    if f.endswith('_id') and rec[f] == 0:
                        rec[f] = None

                rec['create_uid'] = 1
                rec['write_uid'] = 1
                self.b._fill_not_null(rec, tgt_cols)

                cols_q = ', '.join(f'"{c}"' for c in rec)
                placeholders = ', '.join(['%s'] * len(rec))
                try:
                    cur.execute(
                        f'INSERT INTO repair_fee ({cols_q}) VALUES ({placeholders}) RETURNING id',
                        self.b.prepare_vals(rec, tgt_cols),
                    )
                    new_id = cur.fetchone()[0]
                    self.b.id_map['repair_fee'][old_id] = new_id
                    inserted += 1
                except Exception as e:
                    self.b.tgt_conn.rollback()
                    log.error("repair_fee old_id=%s: %s", old_id, e)

        log.info("repair_fee: %d líneas de honorarios migradas.", inserted)

        # M2M impuestos en honorarios
        self._migrate_repair_fee_taxes()

    def _migrate_repair_fee_taxes(self):
        """Migra la relación M2M repair_fee <-> account_tax."""
        for m2m_table in ('repair_fee_account_tax_rel', 'account_tax_repair_fee_rel',
                          'repair_fee_account_tax'):
            if self.b.table_exists_in_src(m2m_table) and self.b.table_exists_in_tgt(m2m_table):
                src_cols = list(self.b.get_src_columns(m2m_table))
                repair_col = next((c for c in src_cols if 'fee' in c or 'repair' in c), None)
                tax_col = next((c for c in src_cols if 'tax' in c), None)
                if repair_col and tax_col:
                    self.b.migrate_m2m(
                        m2m_table, repair_col, tax_col,
                        'repair_fee', 'account_tax'
                    )
                return
