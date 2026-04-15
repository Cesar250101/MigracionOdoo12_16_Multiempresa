"""
migrator_12_16.py
Clase principal de migración Odoo 12 -> Odoo 16 Multiempresa.
Orquesta todos los módulos de migración en el orden correcto.
Sin XML-RPC: conexión directa psycopg2 a ambas BDs.
"""

import logging
import psycopg2
import psycopg2.extras

import config as cfg
from core.base_migrator import BaseMigrator
from modules.accounting import AccountingMigrator
from modules.sales import SalesMigrator
from modules.stock import StockMigrator
from modules.pos import PosMigrator

log = logging.getLogger(__name__)


class Migrator12to16:

    def __init__(self, source_db: dict = None, target_db: dict = None,
                 company_migration: list = None):
        source_db = source_db or cfg.SOURCE_DB
        target_db = target_db or cfg.TARGET_DB
        company_list = company_migration or cfg.COMPANY_MIGRATION

        log.info("Conectando a BD origen (Odoo 12): %s@%s/%s",
                 source_db['user'], source_db['host'], source_db['dbname'])
        self.src_conn = psycopg2.connect(**source_db)

        log.info("Conectando a BD destino (Odoo 16): %s@%s/%s",
                 target_db['user'], target_db['host'], target_db['dbname'])
        self.tgt_conn = psycopg2.connect(**target_db)
        self.tgt_conn.autocommit = True

        # Construir mapeo de empresas {old_id: new_id}
        company_mapping = {}
        for entry in company_list:
            if entry['target_id'] != 0:
                company_mapping[entry['source_id']] = entry['target_id']

        self.base = BaseMigrator(self.src_conn, self.tgt_conn, company_mapping)
        self.accounting = AccountingMigrator(self.base)
        self.sales = SalesMigrator(self.base)
        self.stock = StockMigrator(self.base)
        self.pos = PosMigrator(self.base)

    # ──────────────────────────────────────────────
    # Empresa multiempresa
    # ──────────────────────────────────────────────

    def setup_companies(self):
        """
        Crea empresas en destino si target_id == 0 y actualiza el mapeo.
        Las empresas existentes (target_id != 0) se usan directamente.
        """
        log.info("Configurando empresas (multiempresa)...")
        for entry in cfg.COMPANY_MIGRATION:
            src_id = entry['source_id']
            tgt_id = entry['target_id']

            if tgt_id != 0:
                self.base.company_mapping[src_id] = tgt_id
                log.info("Empresa src=%d -> tgt=%d (existente)", src_id, tgt_id)
                continue

            # Crear nueva empresa en Odoo 16
            company_src = self.base.fetch_src(
                "SELECT name, currency_id, country_id, state_id, city, "
                "       street, street2, zip, phone, email, website, vat "
                "FROM res_company WHERE id=%s",
                (src_id,)
            )
            if not company_src:
                log.warning("Empresa origen id=%d no encontrada.", src_id)
                continue

            c = company_src[0]
            with self.tgt_conn.cursor() as cur:
                try:
                    cur.execute("""
                        INSERT INTO res_company (name, currency_id, create_uid, write_uid)
                        VALUES (%s, %s, 1, 1) RETURNING id
                    """, (entry['name'] or c['name'], 1))
                    new_id = cur.fetchone()[0]
                    self.base.company_mapping[src_id] = new_id
                    log.info("Empresa '%s' creada: tgt_id=%d", entry['name'] or c['name'], new_id)
                except Exception as e:
                    self.tgt_conn.rollback()
                    log.error("Error creando empresa src=%d: %s", src_id, e)

    # ──────────────────────────────────────────────
    # Limpieza previa
    # ──────────────────────────────────────────────

    def clean_target_data(self):
        """Elimina datos previos del destino para las empresas a migrar."""
        if not cfg.CLEAN_BEFORE_MIGRATE:
            log.info("Limpieza deshabilitada (CLEAN_BEFORE_MIGRATE=False).")
            return

        log.info("Limpiando datos previos en destino...")
        company_ids = tuple(self.base.company_mapping.values())
        if not company_ids:
            log.warning("No hay company_ids mapeados, saltando limpieza.")
            return

        with self.tgt_conn.cursor() as cur:
            for table in cfg.TABLES_TO_CLEAN:
                try:
                    cur.execute(
                        "SELECT 1 FROM information_schema.columns "
                        "WHERE table_name=%s AND column_name='company_id' "
                        "AND table_schema='public'",
                        (table,)
                    )
                    if not cur.fetchone():
                        continue

                    placeholders = ', '.join(['%s'] * len(company_ids))
                    cur.execute(
                        f'DELETE FROM "{table}" WHERE company_id IN ({placeholders})',
                        list(company_ids)
                    )
                    log.debug("Limpiado: %s", table)
                except Exception as e:
                    self.tgt_conn.rollback()
                    log.debug("No se pudo limpiar %s: %s", table, e)

        log.info("Limpieza completada.")

    # ──────────────────────────────────────────────
    # Tablas de configuración base
    # ──────────────────────────────────────────────

    def migrate_base_config(self):
        """Migra tablas de configuración sin dependencias externas fuertes."""
        log.info("=== Migrando configuración base ===")

        # Monedas: mapear por nombre (no insertar, solo mapear IDs)
        self._map_currencies()

        # Condiciones de pago
        self.base.migrate_table('account_payment_term')
        self.base.migrate_table('account_payment_term_line',
                                mapping_fields={'payment_id': 'account_payment_term'})

        # Posiciones fiscales
        self.base.migrate_table('account_fiscal_position')
        self.base.migrate_table('account_fiscal_position_tax', mapping_fields={
            'position_id': 'account_fiscal_position',
            'tax_src_id': 'account_tax',
            'tax_dest_id': 'account_tax',
        })
        self.base.migrate_table('account_fiscal_position_account', mapping_fields={
            'position_id': 'account_fiscal_position',
            'account_src_id': 'account_account',
            'account_dest_id': 'account_account',
        })

        # Categorías de producto
        self.base.migrate_table('product_category', is_recursive=True)

        # Grupos de impuesto (para account_tax)
        self.base.migrate_table('account_tax_group')

    def _map_currencies(self):
        """Mapea res_currency del origen al destino por nombre (ISO)."""
        log.info("Mapeando monedas (res_currency)...")
        src_curs = self.base.fetch_src("SELECT id, name FROM res_currency")
        self.base.id_map.setdefault('res_currency', {})
        with self.tgt_conn.cursor() as cur:
            cur.execute("SELECT id, name FROM res_currency")
            tgt_map = {name: tid for tid, name in cur.fetchall()}
        for src_id, src_name in [(r['id'], r['name']) for r in src_curs]:
            if src_name in tgt_map:
                self.base.id_map['res_currency'][src_id] = tgt_map[src_name]
        log.info("res_currency: %d monedas mapeadas.", len(self.base.id_map['res_currency']))

    # ──────────────────────────────────────────────
    # Mapeo de tablas de referencia (datos de localización)
    # ──────────────────────────────────────────────

    def _map_by_code(self, table: str, code_field: str = 'code'):
        """
        Mapea una tabla de referencia (res_country, res_country_state) por su código ISO.
        Los IDs pueden diferir entre versiones de Odoo pero el código es estable.
        """
        src_rows = self.base.fetch_src(f"SELECT id, {code_field} FROM {table}")
        self.base.id_map.setdefault(table, {})
        with self.tgt_conn.cursor() as cur:
            cur.execute(f"SELECT id, {code_field} FROM {table}")
            tgt_map = {code: tid for tid, code in cur.fetchall() if code}
        matched = 0
        for row in src_rows:
            code = row[code_field]
            if code and code in tgt_map:
                self.base.id_map[table][row['id']] = tgt_map[code]
                matched += 1
        log.info("%s: %d/%d registros mapeados por %s.", table, matched, len(src_rows), code_field)

    def _map_res_city(self):
        """
        Mapea res_city entre origen y destino usando nombre + estado.
        Los IDs de ciudades difieren entre versiones de Odoo.
        """
        src_cities = self.base.fetch_src("SELECT id, name, state_id FROM res_city")
        self.base.id_map.setdefault('res_city', {})

        def _city_name(val) -> str:
            """Extrae el nombre de ciudad ya sea string plano o jsonb dict."""
            if isinstance(val, dict):
                return (val.get('en_US') or val.get('es_CL') or
                        next(iter(val.values()), '') or '').lower().strip()
            return (val or '').lower().strip()

        with self.tgt_conn.cursor() as cur:
            cur.execute("SELECT id, name, state_id FROM res_city")
            # Índice: (name_lower, mapped_state_id) -> tgt_id
            tgt_map = {}
            for tid, name, state_id in cur.fetchall():
                name_key = _city_name(name)
                tgt_map[(name_key, state_id)] = tid
                # También indexar solo por nombre como fallback
                if name_key not in tgt_map:
                    tgt_map[name_key] = tid

        state_map = self.base.id_map.get('res_country_state', {})
        matched = 0
        for city in src_cities:
            old_id = city['id']
            name_key = _city_name(city['name'])
            new_state_id = state_map.get(city['state_id'])

            # Intentar match exacto (nombre + estado mapeado)
            tgt_id = tgt_map.get((name_key, new_state_id))
            if tgt_id is None:
                # Fallback: solo por nombre
                tgt_id = tgt_map.get(name_key)

            if tgt_id:
                self.base.id_map['res_city'][old_id] = tgt_id
                matched += 1

        log.info("res_city: %d/%d ciudades mapeadas.", matched, len(src_cities))

    # ──────────────────────────────────────────────
    # Contactos
    # ──────────────────────────────────────────────

    def migrate_partners(self):
        """Migra res.partner (recursivo por parent_id)."""
        log.info("=== Migrando contactos (res_partner) ===")

        # Mapear tablas de referencia por código/nombre (IDs difieren entre versiones)
        self._map_by_code('res_country', 'code')
        self._map_by_code('res_country_state', 'code')
        self._map_res_city()

        src_cols = self.base.get_src_columns('res_partner')
        mapping = {}

        ref_fks = {
            'country_id':  'res_country',
            'state_id':    'res_country_state',
            'city_id':     'res_city',
            'title':       'res_partner_title',
        }
        for fk, ref in ref_fks.items():
            if fk in src_cols:
                # res_partner_title: preload por ID directo (suele coincidir)
                if ref == 'res_partner_title':
                    self.base.preload_id_map(ref)
                mapping[fk] = ref

        self.base.migrate_table(
            'res_partner',
            is_recursive=True,
            mapping_fields=mapping,
            skip_fields=['message_main_attachment_id', 'category_id'],
        )

    # ──────────────────────────────────────────────
    # Productos
    # ──────────────────────────────────────────────

    def migrate_products(self):
        """Migra product.template y product.product."""
        log.info("=== Migrando productos ===")

        # UoM: mapear por nombre
        self._map_uom()

        self.base.migrate_table(
            'product_template',
            mapping_fields={
                'categ_id': 'product_category',
                'uom_id': 'uom_uom',
                'uom_po_id': 'uom_uom',
            },
            skip_fields=['message_main_attachment_id'],
        )

        self.base.migrate_table(
            'product_product',
            mapping_fields={'product_tmpl_id': 'product_template'},
            skip_fields=['message_main_attachment_id'],
        )

        # Rutas de producto (M2M)
        self.base.migrate_m2m(
            'stock_route_product', 'product_id', 'route_id',
            'product_product', 'stock_route'
        )

    def _map_uom(self):
        """Mapea uom_uom del origen al destino por nombre."""
        log.info("Mapeando unidades de medida (uom_uom)...")
        src_uoms = self.base.fetch_src("SELECT id, name FROM uom_uom")
        self.base.id_map.setdefault('uom_uom', {})
        with self.tgt_conn.cursor() as cur:
            cur.execute("SELECT id, name FROM uom_uom")
            tgt_map = {}
            for tid, name in cur.fetchall():
                key = name if not isinstance(name, dict) else name.get('en_US', '')
                tgt_map[key] = tid
        for row in src_uoms:
            src_name = row['name'] if not isinstance(row['name'], dict) else \
                       row['name'].get('en_US', str(row['name']))
            if src_name in tgt_map:
                self.base.id_map['uom_uom'][row['id']] = tgt_map[src_name]

    # ──────────────────────────────────────────────
    # Ventas
    # ──────────────────────────────────────────────

    def migrate_sales(self):
        """Migra sale.order y sale.order.line (delegado a SalesMigrator)."""
        self.sales.migrate_sales()

    # _update_procurement_group_sale movido a modules/sales.py (SalesMigrator)

    # ──────────────────────────────────────────────
    # Compras
    # ──────────────────────────────────────────────

    def migrate_purchases(self):
        """Migra purchase.order y purchase.order.line."""
        log.info("=== Migrando compras ===")

        self.base.migrate_table(
            'purchase_order',
            mapping_fields={
                'partner_id': 'res_partner',
                'dest_address_id': 'res_partner',
                'group_id': 'procurement_group',
                'picking_type_id': 'stock_picking_type',
                'currency_id': 'res_currency',
                'fiscal_position_id': 'account_fiscal_position',
                'payment_term_id': 'account_payment_term',
            },
            skip_fields=['message_main_attachment_id'],
        )

        self.base.migrate_table(
            'purchase_order_line',
            mapping_fields={
                'order_id': 'purchase_order',
                'product_id': 'product_product',
                'partner_id': 'res_partner',
                'product_uom': 'uom_uom',
                'account_analytic_id': None,  # Omitir: analytic cambia en v16
            },
            skip_fields=['account_analytic_id'],
        )
        self.base.migrate_m2m(
            'account_tax_purchase_order_line_rel',
            'purchase_order_line_id', 'account_tax_id',
            'purchase_order_line', 'account_tax'
        )

    # ──────────────────────────────────────────────
    # Secuencias
    # ──────────────────────────────────────────────

    def migrate_sequences(self):
        """
        Migra ir.sequence y su sub-tabla ir.sequence.date.range.
        Reemplaza el migrate_table genérico para incluir la sub-tabla
        y omitir campos que no existen en Odoo 16.
        """
        log.info("=== Migrando secuencias (ir_sequence) ===")

        src_seq_cols = self.base.get_src_columns('ir_sequence')
        skip_seq = {'message_main_attachment_id'}
        # 'date_range_ids' es campo virtual en Odoo 12; en DB es la FK inversa
        skip_seq |= {c for c in src_seq_cols if c not in self.base.get_tgt_columns('ir_sequence')}

        self.base.migrate_table(
            'ir_sequence',
            skip_fields=list(skip_seq),
        )

        # Sub-tabla: rangos de fecha de secuencias (Odoo 12 con use_date_range)
        if self.base.table_exists_in_src('ir_sequence_date_range') and \
                self.base.table_exists_in_tgt('ir_sequence_date_range'):
            self.base.migrate_table(
                'ir_sequence_date_range',
                mapping_fields={'sequence_id': 'ir_sequence'},
            )

    # ──────────────────────────────────────────────
    # CAF localización chilena (dte.caf)
    # ──────────────────────────────────────────────

    def migrate_dte_caf(self):
        """
        Migra la tabla dte.caf (Código de Autorización de Folios) de la
        localización chilena. Detecta automáticamente el nombre de tabla tanto
        en origen como en destino (dte_caf / l10n_cl_dte_caf).
        """
        log.info("=== Migrando CAF (dte.caf) ===")

        # Detectar nombre de tabla en origen
        src_table = None
        for candidate in ('dte_caf', 'l10n_cl_dte_caf'):
            if self.base.table_exists_in_src(candidate):
                src_table = candidate
                break
        if not src_table:
            log.warning("dte.caf: tabla no encontrada en origen (dte_caf / l10n_cl_dte_caf), saltando.")
            return

        # Detectar nombre de tabla en destino
        tgt_table = None
        for candidate in ('dte_caf', 'l10n_cl_dte_caf'):
            if self.base.table_exists_in_tgt(candidate):
                tgt_table = candidate
                break
        if not tgt_table:
            log.warning("dte.caf: tabla no encontrada en destino, saltando.")
            return

        log.info("dte.caf: origen=%s  destino=%s", src_table, tgt_table)

        src_cols = self.base.get_src_columns(src_table)
        tgt_cols = self.base.get_tgt_columns(tgt_table)

        # Construir mapa document_class_id a través de código SII
        doc_class_map = self.accounting._build_sii_doc_class_map()

        rows = self.base.fetch_src(f'SELECT * FROM "{src_table}" ORDER BY id')
        self.base.id_map.setdefault(src_table, {})
        self.base.id_map.setdefault(tgt_table, {})

        # Campos a omitir: los que no existen en destino o son computados
        skip = {'id', 'message_main_attachment_id'}

        inserted = 0
        with self.tgt_conn.cursor() as cur:
            for row in rows:
                old_id = row['id']
                rec = {}

                for col in src_cols:
                    if col in skip or col not in tgt_cols:
                        continue
                    rec[col] = row[col]

                # ── FKs ──────────────────────────────────────────────────
                rec['company_id'] = self.base.map_company(row.get('company_id'))

                if 'journal_id' in src_cols and 'journal_id' in tgt_cols:
                    rec['journal_id'] = self.base.id_map.get('account_journal', {}).get(
                        row.get('journal_id'))

                if 'sequence_id' in src_cols and 'sequence_id' in tgt_cols:
                    rec['sequence_id'] = self.base.id_map.get('ir_sequence', {}).get(
                        row.get('sequence_id'), row.get('sequence_id'))

                if 'document_class_id' in src_cols and 'document_class_id' in tgt_cols:
                    old_dc = row.get('document_class_id')
                    if old_dc:
                        rec['document_class_id'] = doc_class_map.get(old_dc, old_dc) \
                            if doc_class_map else old_dc

                # ── Limpiar FK = 0 ────────────────────────────────────────
                for f in list(rec.keys()):
                    if f.endswith('_id') and rec[f] == 0:
                        rec[f] = None

                rec['create_uid'] = 1
                rec['write_uid'] = 1

                self.base._fill_not_null(rec, tgt_cols)

                cols_q = ', '.join(f'"{c}"' for c in rec)
                placeholders = ', '.join(['%s'] * len(rec))
                try:
                    cur.execute(
                        f'INSERT INTO "{tgt_table}" ({cols_q}) VALUES ({placeholders}) RETURNING id',
                        self.base.prepare_vals(rec, tgt_cols),
                    )
                    new_id = cur.fetchone()[0]
                    self.base.id_map[src_table][old_id] = new_id
                    if src_table != tgt_table:
                        self.base.id_map[tgt_table][old_id] = new_id
                    inserted += 1
                except Exception as e:
                    self.tgt_conn.rollback()
                    log.error("dte_caf old_id=%s: %s", old_id, e)

        log.info("dte_caf: %d registros migrados.", inserted)

    def migrate_sii_firma(self):
        """
        Migra la tabla sii.firma (certificado digital para firma electrónica).
        Detecta automáticamente el nombre de tabla en origen y destino
        (sii_firma / l10n_cl_certificate).
        """
        log.info("=== Migrando sii.firma ===")

        # Detectar tabla en origen
        src_table = None
        for candidate in ('sii_firma', 'l10n_cl_certificate'):
            if self.base.table_exists_in_src(candidate):
                src_table = candidate
                break
        if not src_table:
            log.warning("sii.firma: tabla no encontrada en origen (sii_firma / l10n_cl_certificate), saltando.")
            return

        # Detectar tabla en destino
        tgt_table = None
        for candidate in ('sii_firma', 'l10n_cl_certificate'):
            if self.base.table_exists_in_tgt(candidate):
                tgt_table = candidate
                break
        if not tgt_table:
            log.warning("sii.firma: tabla no encontrada en destino, saltando.")
            return

        log.info("sii.firma: origen=%s  destino=%s", src_table, tgt_table)

        src_cols = self.base.get_src_columns(src_table)
        tgt_cols = self.base.get_tgt_columns(tgt_table)

        rows = self.base.fetch_src(f'SELECT * FROM "{src_table}" ORDER BY id')
        self.base.id_map.setdefault(src_table, {})
        if src_table != tgt_table:
            self.base.id_map.setdefault(tgt_table, {})

        skip = {'id', 'message_main_attachment_id'}
        inserted = 0

        with self.tgt_conn.cursor() as cur:
            for row in rows:
                old_id = row['id']
                rec = {}

                for col in src_cols:
                    if col in skip or col not in tgt_cols:
                        continue
                    rec[col] = row[col]

                rec['company_id'] = self.base.map_company(row.get('company_id'))

                # Limpiar FK = 0
                for f in list(rec.keys()):
                    if f.endswith('_id') and rec[f] == 0:
                        rec[f] = None

                rec['create_uid'] = 1
                rec['write_uid'] = 1

                self.base._fill_not_null(rec, tgt_cols)

                cols_q = ', '.join(f'"{c}"' for c in rec)
                placeholders = ', '.join(['%s'] * len(rec))
                try:
                    cur.execute(
                        f'INSERT INTO "{tgt_table}" ({cols_q}) VALUES ({placeholders}) RETURNING id',
                        self.base.prepare_vals(rec, tgt_cols),
                    )
                    new_id = cur.fetchone()[0]
                    self.base.id_map[src_table][old_id] = new_id
                    if src_table != tgt_table:
                        self.base.id_map[tgt_table][old_id] = new_id
                    inserted += 1
                except Exception as e:
                    self.tgt_conn.rollback()
                    log.error("sii_firma old_id=%s: %s", old_id, e)

        log.info("sii_firma: %d registros migrados.", inserted)

    def run(self):
        """Ejecuta la migración completa en el orden correcto."""
        try:
            log.info("=" * 60)
            log.info("INICIO MIGRACIÓN Odoo 12 -> Odoo 16 Multiempresa")
            log.info("=" * 60)

            # 0. Empresas
            self.setup_companies()

            # 1. Limpieza
            self.clean_target_data()

            # 2. Configuración base (monedas, condiciones de pago, categorías)
            self.migrate_base_config()

            # 3. Contactos
            self.migrate_partners()

            # 4. Plan de cuentas (con transformación de tipos)
            self.accounting.migrate_chart_of_accounts()

            # 5. Impuestos + repartition lines (crítico: antes de facturas)
            self.accounting.migrate_taxes()

            # 6. Diarios
            self.accounting.migrate_journals()

            # 7. Secuencias (ir_sequence + ir_sequence_date_range)
            self.migrate_sequences()

            # 8. Stock: ubicaciones, almacenes, tipos de operación
            self.stock.migrate_locations()
            self.stock.migrate_warehouses()
            self.stock.migrate_picking_types()
            self.stock.migrate_routes()

            # 9. Productos
            self.migrate_products()

            # 10. Ventas
            self.migrate_sales()

            # 11. Albaranes de ventas + movimientos
            self.stock.migrate_pickings()
            self.stock.migrate_lots()          # ANTES de move_lines (FK lot_id)
            self.stock.migrate_moves()
            self.stock.migrate_move_lines()
            if cfg.MIGRATE_STOCK_QUANTS:
                self.stock.migrate_quants()

            # 12. Compras
            self.migrate_purchases()

            # 12b. CAF y firma localización chilena
            self.migrate_sii_firma()
            self.migrate_dte_caf()

            # 13. Facturas (account_invoice -> account_move)
            self.accounting.migrate_invoices()

            # 14. Asientos contables puros (account_move sin factura)
            self.accounting.migrate_journal_entries()

            # 15. Conciliaciones (full_reconcile antes de líneas)
            if cfg.MIGRATE_RECONCILIATION:
                self.base.migrate_table(
                    'account_full_reconcile',
                    mapping_fields={'exchange_move_id': 'account_move'},
                )

            # 16. Líneas contables de asientos puros (account_move_line)
            # NOTA: líneas de producto de facturas se omiten aquí (las migra paso 16b)
            self.accounting.migrate_move_lines()
            self.base.migrate_m2m(
                'account_move_line_account_tax_rel',
                'account_move_line_id', 'account_tax_id',
                'account_move_line', 'account_tax'
            )

            # 16b. Líneas de producto de facturas (account_invoice_line -> account_move_line)
            # Incluye M2M de impuestos de líneas de factura
            self.accounting.migrate_invoice_lines()

            # 17. Vinculación sale_order_line <-> invoice_line
            # invoice_line_id en Odoo 12 apunta a account_invoice_line (no a account_move_line)
            self.base.migrate_m2m(
                'sale_order_line_invoice_rel',
                'order_line_id', 'invoice_line_id',
                'sale_order_line', 'account_invoice_line'
            )

            # 18. Pagos
            self.accounting.migrate_payments()

            # 19. Conciliaciones parciales
            if cfg.MIGRATE_RECONCILIATION:
                self.base.migrate_table(
                    'account_partial_reconcile',
                    mapping_fields={
                        'debit_move_id': 'account_move_line',
                        'credit_move_id': 'account_move_line',
                        'debit_currency_id': 'res_currency',
                        'credit_currency_id': 'res_currency',
                        'full_reconcile_id': 'account_full_reconcile',
                    },
                )

            # 20. POS
            log.info("=== Migrando Punto de Venta ===")
            self.pos.migrate_payment_methods()
            self.pos.migrate_config()
            self.pos.migrate_sessions()
            self.pos.migrate_orders()
            self.pos.migrate_order_lines()
            self.pos.migrate_pos_payments()

            # 21. Post-migración: vincular circulares
            self.accounting.post_migration_updates()
            self.stock.post_migration_stock()

            # 22. Actualizar secuencias PostgreSQL
            self.base.update_sequences()
            self.base.fix_ir_sequences()

            log.info("=" * 60)
            log.info("MIGRACIÓN COMPLETADA EXITOSAMENTE")
            log.info("=" * 60)

        except Exception as e:
            log.error("ERROR FATAL durante migración: %s", e, exc_info=True)
            raise
        finally:
            self.src_conn.close()
            self.tgt_conn.close()
            log.info("Conexiones cerradas.")
