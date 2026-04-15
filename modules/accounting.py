"""
modules/accounting.py
Migración contable Odoo 12 -> Odoo 16.

Cambios críticos manejados aquí:
  • account_account:   user_type_id  -> account_type (selection directa)
  • account_tax:       account_id / refund_account_id -> account_tax_repartition_line
  • account_invoice:   tabla separada -> integrada en account_move (move_type)
  • account_invoice_line: -> account_move_line con display_type correcto
  • account_invoice_tax:  -> account_move_line con display_type='tax'
  • account_payment:   se vincula a account_move via move_id (post-migración)
"""

import logging
from psycopg2.extras import Json

from config import ACCOUNT_TYPE_MAP, ACCOUNT_TYPE_FALLBACK, \
    INVOICE_STATE_MAP, INVOICE_PAYMENT_STATE_MAP

log = logging.getLogger(__name__)


class AccountingMigrator:

    def __init__(self, base):
        """
        Args:
            base: instancia de BaseMigrator (acceso a src_conn, tgt_conn, id_map, etc.)
        """
        self.b = base
        # tax_repr_map[old_tax_id] = {
        #   'invoice_base': new_repr_id,
        #   'invoice_tax':  new_repr_id,
        #   'refund_base':  new_repr_id,
        #   'refund_tax':   new_repr_id,
        # }
        self.tax_repr_map: dict = {}

    # ──────────────────────────────────────────────
    # 1. Plan de cuentas (account.account)
    # ──────────────────────────────────────────────

    def migrate_chart_of_accounts(self):
        """
        Migra account_account convirtiendo user_type_id -> account_type.
        En Odoo 12, el tipo está en account_account_type.type + internal_group.
        En Odoo 16, account_type es un campo selection directo en account_account.
        """
        log.info("Migrando plan de cuentas (account_account)...")

        # Detectar columnas opcionales que pueden no existir en Odoo 12
        src_aa_cols = self.b.get_src_columns('account_account')
        src_aat_cols = self.b.get_src_columns('account_account_type') \
            if self.b.table_exists_in_src('account_account_type') else set()

        # Construir SELECT dinámico con solo columnas existentes en origen
        optional_src = ['active', 'deprecated', 'reconcile', 'note', 'group_id', 'tag_ids',
                        'currency_id']
        extra_cols = ', '.join(
            f'aa.{c}' for c in optional_src if c in src_aa_cols
        )
        if extra_cols:
            extra_cols = ', ' + extra_cols

        internal_group_expr = (
            "COALESCE(aat.internal_group, 'asset') AS internal_group"
            if 'internal_group' in src_aat_cols
            else "'asset' AS internal_group"
        )

        type_rows = self.b.fetch_src(f"""
            SELECT aa.id,
                   aa.code,
                   aa.name,
                   aa.company_id
                   {extra_cols},
                   COALESCE(aat.type, 'other') AS internal_type,
                   {internal_group_expr}
            FROM account_account aa
            LEFT JOIN account_account_type aat ON aat.id = aa.user_type_id
            ORDER BY aa.id
        """)

        # Columnas que existen en destino
        tgt_cols = self.b.get_tgt_columns('account_account')

        self.b.id_map.setdefault('account_account', {})
        inserted = 0

        with self.b.tgt_conn.cursor() as cur:
            for row in type_rows:
                old_id = row['id']

                # Mapear tipo al formato Odoo 16
                account_type = ACCOUNT_TYPE_MAP.get(
                    (row['internal_type'], row['internal_group']),
                    ACCOUNT_TYPE_MAP.get((row['internal_type'], None), ACCOUNT_TYPE_FALLBACK)
                )

                # Guardar en caché para identificar cuentas receivable/payable
                self.b.account_type_cache[old_id] = account_type

                new_company_id = self.b.map_company(row['company_id'])
                new_currency_id = self.b.id_map.get('res_currency', {}).get(
                    row['currency_id']) if 'currency_id' in src_aa_cols else None

                rec = {
                    'code': row['code'],
                    'name': row['name'],
                    'account_type': account_type,
                    'company_id': new_company_id,
                    'currency_id': new_currency_id,
                    'active': row['active'] if 'active' in src_aa_cols else True,
                    'deprecated': row['deprecated'] if 'deprecated' in src_aa_cols else False,
                    'reconcile': row['reconcile'] if 'reconcile' in src_aa_cols else False,
                    'note': row['note'] if 'note' in src_aa_cols else '',
                    'create_uid': 1,
                    'write_uid': 1,
                }

                # Filtrar campos que no existen en destino
                rec = {k: v for k, v in rec.items() if k in tgt_cols}

                # group_id: si existe en destino y tenemos mapeo
                if 'group_id' in tgt_cols and 'group_id' in src_aa_cols and row.get('group_id'):
                    rec['group_id'] = self.b.id_map.get('account_group', {}).get(row['group_id'])

                cols_q = ', '.join(f'"{c}"' for c in rec)
                placeholders = ', '.join(['%s'] * len(rec))
                try:
                    cur.execute(
                        f'INSERT INTO account_account ({cols_q}) '
                        f'VALUES ({placeholders}) RETURNING id',
                        self.b.prepare_vals(rec, tgt_cols)
                    )
                    new_id = cur.fetchone()[0]
                    self.b.id_map['account_account'][old_id] = new_id
                    inserted += 1
                except Exception as e:
                    self.b.tgt_conn.rollback()
                    existing = self.b._find_existing('account_account', rec, cur)
                    if existing:
                        self.b.id_map['account_account'][old_id] = existing
                        self.b.account_type_cache[existing] = account_type
                    else:
                        log.error("account_account old_id=%s: %s", old_id, e)

        log.info("account_account: %d cuentas migradas.", inserted)

    # ──────────────────────────────────────────────
    # 2. Impuestos + líneas de distribución
    # ──────────────────────────────────────────────

    def migrate_taxes(self):
        """
        Migra account_tax y crea account_tax_repartition_line para Odoo 16.
        En Odoo 12 los taxes tienen account_id y refund_account_id directos.
        En Odoo 16 esto se maneja mediante repartition lines.
        """
        log.info("Migrando impuestos (account_tax + repartition lines)...")

        taxes = self.b.fetch_src("""
            SELECT id, name, type_tax_use, amount_type, active, company_id,
                   amount, description, price_include, include_base_amount,
                   analytic, tax_group_id, sequence,
                   account_id, refund_account_id
            FROM account_tax
            ORDER BY id
        """)

        tgt_tax_cols = self.b.get_tgt_columns('account_tax')
        tgt_repr_cols = self.b.get_tgt_columns('account_tax_repartition_line')
        self.b.id_map.setdefault('account_tax', {})
        self.b.id_map.setdefault('account_tax_repartition_line', {})

        # Construir mapa company_id -> country_id desde res_company/res_partner en destino
        company_country_map = {}
        with self.b.tgt_conn.cursor() as cur:
            cur.execute("""
                SELECT c.id, p.country_id
                FROM res_company c
                JOIN res_partner p ON p.id = c.partner_id
            """)
            for row in cur.fetchall():
                company_country_map[row[0]] = row[1]

        inserted_taxes = 0
        inserted_repr = 0

        with self.b.tgt_conn.cursor() as cur:
            for tax in taxes:
                old_id = tax['id']
                new_company_id = self.b.map_company(tax['company_id'])
                new_account_id = self.b.id_map.get('account_account', {}).get(tax['account_id'])
                new_refund_account_id = self.b.id_map.get('account_account', {}).get(tax['refund_account_id'])

                tax_rec = {
                    'name': tax['name'],
                    'type_tax_use': tax['type_tax_use'] or 'none',
                    'amount_type': tax['amount_type'] or 'percent',
                    'active': tax.get('active', True),
                    'company_id': new_company_id,
                    'amount': tax.get('amount', 0),
                    'description': tax.get('description', ''),
                    'price_include': tax.get('price_include', False),
                    'include_base_amount': tax.get('include_base_amount', False),
                    'sequence': tax.get('sequence', 10),
                    'create_uid': 1,
                    'write_uid': 1,
                }

                # country_id es NOT NULL en Odoo 16: viene de la empresa destino
                if 'country_id' in tgt_tax_cols:
                    tax_rec['country_id'] = company_country_map.get(new_company_id)

                # Campos que pueden no existir en Odoo 16
                if 'analytic' in tgt_tax_cols:
                    tax_rec['analytic'] = tax.get('analytic', False)
                if 'tax_group_id' in tgt_tax_cols and tax.get('tax_group_id'):
                    tax_rec['tax_group_id'] = self.b.id_map.get('account_tax_group', {}).get(
                        tax['tax_group_id'])

                cols_q = ', '.join(f'"{c}"' for c in tax_rec)
                placeholders = ', '.join(['%s'] * len(tax_rec))
                try:
                    cur.execute(
                        f'INSERT INTO account_tax ({cols_q}) VALUES ({placeholders}) RETURNING id',
                        self.b.prepare_vals(tax_rec, tgt_tax_cols)
                    )
                    new_tax_id = cur.fetchone()[0]
                    self.b.id_map['account_tax'][old_id] = new_tax_id
                    inserted_taxes += 1
                except Exception as e:
                    self.b.tgt_conn.rollback()
                    existing = self.b._find_existing('account_tax', tax_rec, cur)
                    if existing:
                        self.b.id_map['account_tax'][old_id] = existing
                        new_tax_id = existing
                    else:
                        log.error("account_tax old_id=%s: %s", old_id, e)
                        continue

                # Crear repartition lines (4 por impuesto: invoice/refund × base/tax)
                repr_ids = {}
                for doc_type in ('invoice', 'refund'):
                    account_id = new_account_id if doc_type == 'invoice' else new_refund_account_id
                    for repr_type in ('base', 'tax'):
                        is_tax_line = repr_type == 'tax'
                        repr_rec = {
                            'repartition_type': repr_type,
                            'factor_percent': 100.0,
                            'account_id': account_id if is_tax_line else None,
                            'company_id': new_company_id,
                            'sequence': 1 if repr_type == 'base' else 2,
                            'create_uid': 1,
                            'write_uid': 1,
                        }
                        if doc_type == 'invoice':
                            repr_rec['invoice_tax_id'] = new_tax_id
                            repr_rec['refund_tax_id'] = None
                        else:
                            repr_rec['invoice_tax_id'] = None
                            repr_rec['refund_tax_id'] = new_tax_id

                        # Filtrar columnas que existen en destino
                        repr_rec = {k: v for k, v in repr_rec.items() if k in tgt_repr_cols or k in ('create_uid', 'write_uid')}

                        cols_r = ', '.join(f'"{c}"' for c in repr_rec)
                        ph_r = ', '.join(['%s'] * len(repr_rec))
                        try:
                            cur.execute(
                                f'INSERT INTO account_tax_repartition_line ({cols_r}) '
                                f'VALUES ({ph_r}) RETURNING id',
                                self.b.prepare_vals(repr_rec, tgt_repr_cols)
                            )
                            new_repr_id = cur.fetchone()[0]
                            key = f'{doc_type}_{repr_type}'
                            repr_ids[key] = new_repr_id
                            inserted_repr += 1
                        except Exception as e:
                            self.b.tgt_conn.rollback()
                            log.warning("repartition_line tax_id=%s %s_%s: %s",
                                        new_tax_id, doc_type, repr_type, e)

                self.tax_repr_map[old_id] = repr_ids

        log.info("account_tax: %d impuestos, %d repartition lines.", inserted_taxes, inserted_repr)

    # ──────────────────────────────────────────────
    # 3. Diarios (account_journal)
    # ──────────────────────────────────────────────

    def migrate_journals(self):
        """
        Migra account_journal. En Odoo 12 algunos campos difieren de Odoo 16
        (default_debit_account_id / default_credit_account_id -> default_account_id).
        """
        log.info("Migrando diarios (account_journal)...")

        src_cols = self.b.get_src_columns('account_journal')
        tgt_cols = self.b.get_tgt_columns('account_journal')

        journals = self.b.fetch_src("SELECT * FROM account_journal ORDER BY id")
        self.b.id_map.setdefault('account_journal', {})

        inserted = 0
        with self.b.tgt_conn.cursor() as cur:
            for j in journals:
                old_id = j['id']
                new_company_id = self.b.map_company(j.get('company_id'))

                rec = {
                    'name': j['name'],
                    'code': j['code'],
                    'type': j['type'],
                    'active': j.get('active', True),
                    'company_id': new_company_id,
                    'sequence': j.get('sequence', 10),
                    'create_uid': 1,
                    'write_uid': 1,
                }

                # Mapear currency_id
                if j.get('currency_id'):
                    rec['currency_id'] = self.b.id_map.get('res_currency', {}).get(j['currency_id'])

                # Odoo 12: default_debit_account_id / default_credit_account_id
                # Odoo 16: default_account_id
                if 'default_account_id' in tgt_cols:
                    src_acc = j.get('default_debit_account_id') or j.get('default_account_id')
                    if src_acc:
                        rec['default_account_id'] = self.b.id_map.get('account_account', {}).get(src_acc)
                elif 'default_debit_account_id' in tgt_cols:
                    for fld in ('default_debit_account_id', 'default_credit_account_id'):
                        if j.get(fld):
                            rec[fld] = self.b.id_map.get('account_account', {}).get(j[fld])

                # Campos opcionales que coincidan en src y tgt
                for fld in ('show_on_dashboard', 'color', 'restrict_mode_hash_table',
                            'refund_sequence', 'invoice_reference_type',
                            'invoice_reference_model'):
                    if fld in src_cols and fld in tgt_cols:
                        rec[fld] = j.get(fld)

                # Campos NOT NULL nuevos en Odoo 16 que no existen en Odoo 12
                journal_not_null_defaults = {
                    'invoice_reference_type':  'invoice',
                    'invoice_reference_model': 'invoice',
                    'reconcile_mode':          'edit',   # 'edit' o 'keep'
                }
                for fld, default_val in journal_not_null_defaults.items():
                    if fld in tgt_cols and not rec.get(fld):
                        rec[fld] = default_val

                cols_q = ', '.join(f'"{c}"' for c in rec)
                placeholders = ', '.join(['%s'] * len(rec))
                try:
                    cur.execute(
                        f'INSERT INTO account_journal ({cols_q}) VALUES ({placeholders}) RETURNING id',
                        self.b.prepare_vals(rec, tgt_cols)
                    )
                    new_id = cur.fetchone()[0]
                    self.b.id_map['account_journal'][old_id] = new_id
                    inserted += 1
                except Exception as e:
                    self.b.tgt_conn.rollback()
                    existing = self.b._find_existing('account_journal', rec, cur)
                    if existing:
                        self.b.id_map['account_journal'][old_id] = existing
                    else:
                        log.error("account_journal old_id=%s: %s", old_id, e)

        log.info("account_journal: %d diarios migrados.", inserted)

    # ──────────────────────────────────────────────
    # 4. Facturas: account_invoice -> account_move
    # ──────────────────────────────────────────────

    def migrate_invoices(self):
        """
        En Odoo 12: account_invoice es una tabla separada que enlaza a account_move.
        En Odoo 16: account_move incorpora las facturas directamente (move_type != 'entry').

        Estrategia:
          1. Leer account_invoice + JOIN account_move para obtener los datos contables.
          2. Crear account_move en Odoo 16 con move_type = invoice.type.
          3. Mapear el old account_invoice.id -> new account_move.id.
          4. Mapear el old account_invoice.move_id -> new account_move.id.
             (para que las líneas contables se vinculen correctamente)
        """
        log.info("Migrando facturas (account_invoice -> account_move)...")

        invoices = self.b.fetch_src("""
            SELECT
                ai.id                       AS inv_id,
                ai.type                     AS move_type,
                ai.number                   AS inv_number,
                ai.move_name,
                ai.reference                AS ref,
                ai.origin                   AS invoice_origin,
                ai.comment                  AS narration,
                ai.state                    AS inv_state,
                ai.date_invoice             AS invoice_date,
                ai.date_due                 AS invoice_date_due,
                ai.partner_id,
                ai.payment_term_id          AS invoice_payment_term_id,
                ai.fiscal_position_id,
                ai.user_id                  AS invoice_user_id,
                ai.company_id,
                ai.currency_id,
                ai.commercial_partner_id,
                ai.partner_shipping_id,
                ai.sent                     AS invoice_sent,
                am.id                       AS old_move_id,
                am.journal_id,
                am.date,
                am.name                     AS move_name_journal,
                am.ref                      AS move_ref
            FROM account_invoice ai
            LEFT JOIN account_move am ON am.id = ai.move_id
            ORDER BY ai.id
        """)

        self.b.id_map.setdefault('account_invoice', {})
        self.b.id_map.setdefault('account_move', {})
        tgt_move_cols = self.b.get_tgt_columns('account_move')

        inserted = 0
        with self.b.tgt_conn.cursor() as cur:
            for inv in invoices:
                old_inv_id = inv['inv_id']
                old_move_id = inv['old_move_id']

                inv_state = inv['inv_state'] or 'draft'
                new_state = INVOICE_STATE_MAP.get(inv_state, 'draft')
                payment_state = INVOICE_PAYMENT_STATE_MAP.get(inv_state, 'not_paid')

                # Nombre del asiento: usar number si existe, si no move_name
                move_name = (inv['inv_number'] or inv['move_name'] or
                             inv['move_name_journal'] or '/')

                new_journal_id = self.b.id_map.get('account_journal', {}).get(inv['journal_id'])
                new_partner_id = self.b.id_map.get('res_partner', {}).get(inv['partner_id'])
                new_company_id = self.b.map_company(inv['company_id'])
                new_currency_id = self.b.id_map.get('res_currency', {}).get(inv['currency_id'])
                new_payment_term_id = self.b.id_map.get('account_payment_term', {}).get(
                    inv['invoice_payment_term_id'])
                new_fiscal_pos_id = self.b.id_map.get('account_fiscal_position', {}).get(
                    inv['fiscal_position_id'])
                new_comm_partner = self.b.id_map.get('res_partner', {}).get(
                    inv['commercial_partner_id'])
                new_ship_partner = self.b.id_map.get('res_partner', {}).get(
                    inv['partner_shipping_id'])

                rec = {
                    'name': move_name,
                    'move_type': inv['move_type'] or 'out_invoice',
                    'state': new_state,
                    'ref': inv['ref'] or inv['move_ref'] or '',
                    'invoice_origin': inv['invoice_origin'] or '',
                    'narration': inv['narration'] or '',
                    'invoice_date': inv['invoice_date'],
                    'invoice_date_due': inv['invoice_date_due'],
                    'date': inv['date'],
                    'partner_id': new_partner_id,
                    'journal_id': new_journal_id,
                    'company_id': new_company_id,
                    'currency_id': new_currency_id,
                    'invoice_payment_term_id': new_payment_term_id,
                    'fiscal_position_id': new_fiscal_pos_id,
                    'commercial_partner_id': new_comm_partner,
                    'partner_shipping_id': new_ship_partner,
                    'payment_state': payment_state,
                    'create_uid': 1,
                    'write_uid': 1,
                }

                # Campos opcionales de Odoo 16
                if 'invoice_user_id' in tgt_move_cols:
                    rec['invoice_user_id'] = 1  # Admin fallback
                if 'invoice_sent' in tgt_move_cols:
                    rec['invoice_sent'] = inv.get('invoice_sent', False)
                # auto_post es NOT NULL en Odoo 16, valor por defecto 'no'
                if 'auto_post' in tgt_move_cols:
                    rec['auto_post'] = 'no'

                # Filtrar solo los que existen en destino
                rec = {k: v for k, v in rec.items() if k in tgt_move_cols}

                cols_q = ', '.join(f'"{c}"' for c in rec)
                placeholders = ', '.join(['%s'] * len(rec))
                try:
                    cur.execute(
                        f'INSERT INTO account_move ({cols_q}) VALUES ({placeholders}) RETURNING id',
                        self.b.prepare_vals(rec, tgt_move_cols)
                    )
                    new_move_id = cur.fetchone()[0]
                    self.b.id_map['account_invoice'][old_inv_id] = new_move_id
                    # CRÍTICO: mapear también el old_move_id para vincular líneas contables
                    if old_move_id:
                        self.b.id_map['account_move'][old_move_id] = new_move_id
                    inserted += 1
                except Exception as e:
                    self.b.tgt_conn.rollback()
                    log.error("account_invoice old_id=%s: %s", old_inv_id, e)

        log.info("account_invoice->account_move: %d facturas migradas.", inserted)

    # ──────────────────────────────────────────────
    # 5. Asientos no-factura: account_move 'entry'
    # ──────────────────────────────────────────────

    def migrate_journal_entries(self):
        """
        Migra los account_move de Odoo 12 que NO están vinculados a facturas.
        Estos son asientos manuales, pagos, etc. Se crean con move_type='entry'.
        """
        log.info("Migrando asientos contables puros (account_move 'entry')...")

        entries = self.b.fetch_src("""
            SELECT am.*
            FROM account_move am
            WHERE am.id NOT IN (
                SELECT COALESCE(move_id, 0) FROM account_invoice WHERE move_id IS NOT NULL
            )
            ORDER BY am.id
        """)

        tgt_move_cols = self.b.get_tgt_columns('account_move')
        src_move_cols = self.b.get_src_columns('account_move')
        skip = {'id'}
        cols_to_copy = [c for c in tgt_move_cols if c != 'id' and c in src_move_cols and c not in skip]

        inserted = 0
        with self.b.tgt_conn.cursor() as cur:
            for entry in entries:
                old_id = entry['id']
                # Si ya fue mapeado (como factura), saltar
                if old_id in self.b.id_map.get('account_move', {}):
                    continue

                rec = {}
                for c in cols_to_copy:
                    rec[c] = entry[c]

                # Forzar move_type='entry' para asientos puros
                rec['move_type'] = 'entry'
                rec['company_id'] = self.b.map_company(entry.get('company_id'))
                rec['create_uid'] = 1
                rec['write_uid'] = 1

                # Mapear FKs
                for fk_field, ref_table in [
                    ('journal_id', 'account_journal'),
                    ('partner_id', 'res_partner'),
                    ('currency_id', 'res_currency'),
                    ('fiscal_position_id', 'account_fiscal_position'),
                    ('invoice_payment_term_id', 'account_payment_term'),
                    ('commercial_partner_id', 'res_partner'),
                ]:
                    if fk_field in rec and rec[fk_field]:
                        rec[fk_field] = self.b.id_map.get(ref_table, {}).get(rec[fk_field])

                # Limpiar FKs inválidas
                for f in list(rec.keys()):
                    if f.endswith('_id') and rec[f] == 0:
                        rec[f] = None

                cols_q = ', '.join(f'"{c}"' for c in rec)
                placeholders = ', '.join(['%s'] * len(rec))
                try:
                    cur.execute(
                        f'INSERT INTO account_move ({cols_q}) VALUES ({placeholders}) RETURNING id',
                        self.b.prepare_vals(rec, tgt_move_cols)
                    )
                    new_id = cur.fetchone()[0]
                    self.b.id_map.setdefault('account_move', {})[old_id] = new_id
                    inserted += 1
                except Exception as e:
                    self.b.tgt_conn.rollback()
                    log.error("account_move entry old_id=%s: %s", old_id, e)

        log.info("account_move entries: %d asientos migrados.", inserted)

    # ──────────────────────────────────────────────
    # 6. Líneas contables: account_move_line
    # ──────────────────────────────────────────────

    def migrate_move_lines(self):
        """
        Migra account_move_line de Odoo 12 a Odoo 16.

        Transformaciones clave:
          • Detecta display_type: 'tax' si tax_line_id, 'payment_term' si cuenta receivable/payable,
            False para líneas de producto/genéricas.
          • Agrega tax_repartition_line_id para líneas de impuesto.
          • Mapea product_uom_id (era uom_id en algunas versiones).
        """
        log.info("Migrando líneas contables (account_move_line)...")

        # Mapa company_id -> currency_id para usar como fallback en líneas sin moneda
        company_currency_map = {}
        with self.b.tgt_conn.cursor() as cur:
            cur.execute("SELECT id, currency_id FROM res_company")
            for row in cur.fetchall():
                if row[1]:
                    company_currency_map[row[0]] = row[1]

        src_cols = self.b.get_src_columns('account_move_line')
        tgt_cols = self.b.get_tgt_columns('account_move_line')

        # Campos a omitir del origen (no existen o tienen diferente semántica en v16)
        skip_src = {
            'id', 'invoice_id',           # se usa move_id ahora
            'statement_id', 'statement_line_id',  # bank statements differ
            'analytic_account_id',         # -> analytic_distribution (JSON)
        }

        cols_to_copy = [c for c in tgt_cols if c != 'id' and c in src_cols and c not in skip_src]

        lines = self.b.fetch_src("""
            SELECT aml.*,
                   am2.type     AS inv_type,   -- Odoo 12: account_invoice.type
                   am2.state    AS inv_state
            FROM account_move_line aml
            LEFT JOIN account_invoice am2 ON am2.move_id = aml.move_id
            ORDER BY aml.move_id, aml.id
        """)

        self.b.id_map.setdefault('account_move_line', {})
        inserted = 0
        skipped = 0

        with self.b.tgt_conn.cursor() as cur:
            for line in lines:
                old_id = line['id']
                old_move_id = line['move_id']

                new_move_id = self.b.id_map.get('account_move', {}).get(old_move_id)
                if not new_move_id:
                    skipped += 1
                    continue

                # Saltar líneas de producto de asientos vinculados a facturas.
                # Esas líneas serán migradas desde account_invoice_line por
                # migrate_invoice_lines() con los datos completos (product_id, qty, etc.).
                # Solo conservamos tax y payment_term de los asientos de factura.
                inv_type = line.get('inv_type')
                if inv_type is not None:
                    old_tax_line_id = line.get('tax_line_id')
                    old_acc_id      = line.get('account_id')
                    acc_type        = self.b.account_type_cache.get(old_acc_id, '') \
                                      if old_acc_id else ''
                    if not old_tax_line_id and acc_type not in (
                        'asset_receivable', 'liability_payable'
                    ):
                        skipped += 1
                        continue

                rec = {}
                for c in cols_to_copy:
                    rec[c] = line[c]

                rec['move_id'] = new_move_id
                rec['company_id'] = self.b.map_company(line.get('company_id'))
                rec['create_uid'] = 1
                rec['write_uid'] = 1

                # Mapear FKs estándar
                for fk, ref in [
                    ('account_id', 'account_account'),
                    ('partner_id', 'res_partner'),
                    ('journal_id', 'account_journal'),
                    ('currency_id', 'res_currency'),
                    ('company_currency_id', 'res_currency'),
                    ('product_id', 'product_product'),
                    ('product_uom_id', 'uom_uom'),
                    ('tax_line_id', 'account_tax'),
                    ('full_reconcile_id', 'account_full_reconcile'),
                ]:
                    if fk in rec and rec[fk]:
                        rec[fk] = self.b.id_map.get(ref, {}).get(rec[fk])

                # tax_ids M2M se maneja después con migrate_m2m

                # Determinar display_type
                display_type = self._detect_display_type(line, rec)
                if 'display_type' in tgt_cols:
                    rec['display_type'] = display_type

                # tax_repartition_line_id: obligatorio para líneas de impuesto en Odoo 16
                if display_type == 'tax' and 'tax_repartition_line_id' in tgt_cols:
                    repr_id = self._get_repartition_line_id(line)
                    rec['tax_repartition_line_id'] = repr_id

                # Limpiar FK = 0
                for f in list(rec.keys()):
                    if f.endswith('_id') and rec[f] == 0:
                        rec[f] = None

                # currency_id es NOT NULL en Odoo 16: fallback a moneda de la empresa
                if not rec.get('currency_id') and 'currency_id' in tgt_cols:
                    rec['currency_id'] = company_currency_map.get(
                        rec.get('company_id'), next(iter(company_currency_map.values()), None)
                    )

                # Rellenar NOT NULL
                self.b._fill_not_null(rec, tgt_cols)

                cols_q = ', '.join(f'"{c}"' for c in rec)
                placeholders = ', '.join(['%s'] * len(rec))
                try:
                    cur.execute(
                        f'INSERT INTO account_move_line ({cols_q}) VALUES ({placeholders}) RETURNING id',
                        self.b.prepare_vals(rec, tgt_cols)
                    )
                    new_id = cur.fetchone()[0]
                    self.b.id_map['account_move_line'][old_id] = new_id
                    inserted += 1
                except Exception as e:
                    self.b.tgt_conn.rollback()
                    log.error("account_move_line old_id=%s: %s", old_id, e)

        log.info("account_move_line: %d líneas migradas, %d saltadas (move no mapeado).",
                 inserted, skipped)

    def _detect_display_type(self, src_line, rec: dict) -> str:
        """
        Detecta el display_type para Odoo 16 basado en la línea de Odoo 12.
        """
        # Línea de impuesto: tiene tax_line_id en origen
        old_tax_line_id = src_line.get('tax_line_id')
        if old_tax_line_id:
            return 'tax'

        # Línea de cobro/pago: cuenta receivable o payable
        old_account_id = src_line.get('account_id')
        if old_account_id:
            acc_type = self.b.account_type_cache.get(old_account_id, '')
            if acc_type in ('asset_receivable', 'liability_payable'):
                return 'payment_term'

        # Por defecto: línea de producto/genérica
        return False

    def _get_repartition_line_id(self, src_line) -> int:
        """
        Retorna el nuevo tax_repartition_line_id para una línea de impuesto.
        Determina si es invoice o refund basado en el tipo de asiento.
        """
        old_tax_id = src_line.get('tax_line_id')
        if not old_tax_id or old_tax_id not in self.tax_repr_map:
            return None

        repr_ids = self.tax_repr_map[old_tax_id]
        inv_type = src_line.get('inv_type', '')

        # out_refund / in_refund -> usar repartition line de refund
        if inv_type in ('out_refund', 'in_refund'):
            return repr_ids.get('refund_tax')
        else:
            return repr_ids.get('invoice_tax')

    # ──────────────────────────────────────────────
    # 6b. Líneas de factura: account_invoice_line -> account_move_line
    # ──────────────────────────────────────────────

    def migrate_invoice_lines(self):
        """
        Migra account_invoice_line de Odoo 12 -> account_move_line en Odoo 16.

        En Odoo 12 las líneas de producto de la factura están en account_invoice_line.
        En Odoo 16 esas mismas líneas residen en account_move_line con display_type=False.
        Estas son las líneas visibles/editables en la UI de la factura (producto, cantidad,
        precio, descuento, etc.).

        Nota: migrate_move_lines() ya omite las líneas de producto de los asientos de
        factura (solo conserva tax y payment_term) para evitar duplicados.
        """
        log.info("Migrando líneas de factura (account_invoice_line -> account_move_line)...")

        if not self.b.table_exists_in_src('account_invoice_line'):
            log.warning("account_invoice_line no existe en origen, saltando.")
            return

        src_cols = self.b.get_src_columns('account_invoice_line')
        tgt_cols = self.b.get_tgt_columns('account_move_line')

        # Moneda por empresa en destino (fallback cuando la línea no tiene currency_id)
        company_currency_map = {}
        with self.b.tgt_conn.cursor() as cur:
            cur.execute("SELECT id, currency_id FROM res_company")
            for cid, ccur in cur.fetchall():
                if ccur:
                    company_currency_map[cid] = ccur

        lines = self.b.fetch_src("""
            SELECT ail.*,
                   ai.partner_id  AS inv_partner_id,
                   ai.company_id  AS inv_company_id,
                   ai.currency_id AS inv_currency_id,
                   ai.type        AS inv_type
            FROM account_invoice_line ail
            JOIN account_invoice ai ON ai.id = ail.invoice_id
            ORDER BY ail.invoice_id, ail.sequence, ail.id
        """)

        self.b.id_map.setdefault('account_invoice_line', {})
        inserted = 0
        skipped  = 0

        with self.b.tgt_conn.cursor() as cur:
            for line in lines:
                old_id         = line['id']
                old_invoice_id = line['invoice_id']

                # Factura debe estar mapeada
                new_move_id = self.b.id_map.get('account_invoice', {}).get(old_invoice_id)
                if not new_move_id:
                    skipped += 1
                    continue

                new_company_id  = self.b.map_company(line['inv_company_id'])
                new_currency_id = (
                    self.b.id_map.get('res_currency', {}).get(line['inv_currency_id'])
                    or company_currency_map.get(new_company_id)
                )

                # display_type: Odoo 12 normalmente no tiene este campo en invoice_line
                display_type = (
                    line.get('display_type')
                    if 'display_type' in src_cols
                    else False
                )
                if display_type not in ('line_section', 'line_note'):
                    display_type = False   # línea de producto estándar

                # Calcular balance / debit / credit a partir del importe y tipo de factura
                price_subtotal = float(line.get('price_subtotal') or 0.0)
                inv_type       = line.get('inv_type') or 'out_invoice'

                if 'price_subtotal_signed' in src_cols and line.get('price_subtotal_signed') is not None:
                    balance = float(line['price_subtotal_signed'])
                elif inv_type in ('out_invoice', 'in_refund'):
                    balance = -price_subtotal   # crédito en cuenta de ingresos
                else:
                    balance = price_subtotal    # débito en cuenta de gastos

                debit           = max(0.0, balance)
                credit          = max(0.0, -balance)
                amount_currency = balance       # misma divisa por defecto

                rec = {
                    'move_id':          new_move_id,
                    'display_type':     display_type,
                    'name':             line.get('name') or '',
                    'sequence':         line.get('sequence') or 10,
                    'account_id':       self.b.id_map.get('account_account',  {}).get(line.get('account_id')),
                    'product_id':       self.b.id_map.get('product_product',  {}).get(line.get('product_id')),
                    'product_uom_id':   self.b.id_map.get('uom_uom',          {}).get(line.get('uom_id')),
                    'quantity':         float(line.get('quantity') or 0.0),
                    'price_unit':       float(line.get('price_unit') or 0.0),
                    'discount':         float(line.get('discount') or 0.0),
                    'price_subtotal':   abs(price_subtotal),
                    'price_total':      abs(float(line.get('price_total') or price_subtotal)),
                    'debit':            debit,
                    'credit':           credit,
                    'balance':          balance,
                    'amount_currency':  amount_currency,
                    'partner_id':       self.b.id_map.get('res_partner', {}).get(line.get('inv_partner_id')),
                    'company_id':       new_company_id,
                    'currency_id':      new_currency_id,
                    'company_currency_id': company_currency_map.get(new_company_id, new_currency_id),
                    'create_uid':       1,
                    'write_uid':        1,
                }

                # Conservar solo campos que existen en destino
                rec = {k: v for k, v in rec.items() if k in tgt_cols}

                # FK=0 -> NULL
                for f in list(rec.keys()):
                    if f.endswith('_id') and rec[f] == 0:
                        rec[f] = None

                self.b._fill_not_null(rec, tgt_cols)

                cols_q       = ', '.join(f'"{c}"' for c in rec)
                placeholders = ', '.join(['%s'] * len(rec))
                try:
                    cur.execute(
                        f'INSERT INTO account_move_line ({cols_q}) VALUES ({placeholders}) RETURNING id',
                        self.b.prepare_vals(rec, tgt_cols),
                    )
                    new_id = cur.fetchone()[0]
                    self.b.id_map['account_invoice_line'][old_id] = new_id
                    inserted += 1
                except Exception as e:
                    self.b.tgt_conn.rollback()
                    log.error("account_invoice_line old_id=%s: %s", old_id, e)

        log.info(
            "account_invoice_line->account_move_line: %d líneas migradas, %d saltadas.",
            inserted, skipped,
        )

        # M2M: impuestos de líneas de factura
        self._migrate_invoice_line_taxes()

    def _migrate_invoice_line_taxes(self):
        """
        Migra account_invoice_line_tax (Odoo 12) ->
        account_move_line_account_tax_rel (Odoo 16).

        Odoo 12: invoice_line_id  -> account_invoice_line.id
                 tax_id           -> account_tax.id
        Odoo 16: account_move_line_id -> account_move_line.id  (mapeado via account_invoice_line)
                 account_tax_id       -> account_tax.id
        """
        src_table = 'account_invoice_line_tax'
        tgt_table = 'account_move_line_account_tax_rel'

        if not self.b.table_exists_in_src(src_table):
            log.warning("%s no existe en origen, saltando.", src_table)
            return

        with self.b.src_conn.cursor() as cur:
            cur.execute(f'SELECT invoice_line_id, tax_id FROM "{src_table}"')
            rows = cur.fetchall()

        inserted = 0
        with self.b.tgt_conn.cursor() as tgt_cur:
            for inv_line_id, tax_id in rows:
                new_line_id = self.b.id_map.get('account_invoice_line', {}).get(inv_line_id)
                new_tax_id  = self.b.id_map.get('account_tax',           {}).get(tax_id)
                if not new_line_id or not new_tax_id:
                    continue
                try:
                    tgt_cur.execute(
                        f'INSERT INTO "{tgt_table}" (account_move_line_id, account_tax_id) '
                        f'VALUES (%s, %s) ON CONFLICT DO NOTHING',
                        (new_line_id, new_tax_id),
                    )
                    inserted += 1
                except Exception:
                    self.b.tgt_conn.rollback()

        log.info(
            "%s -> %s: %d relaciones migradas.", src_table, tgt_table, inserted,
        )

    # ──────────────────────────────────────────────
    # 7. Pagos (account_payment)
    # ──────────────────────────────────────────────

    def migrate_payments(self):
        """
        Migra account_payment. En Odoo 16, account_payment.move_id apunta al
        journal entry asociado. En Odoo 12 la relación es inversa via move_line.
        Se vincula el move_id en la fase post-migración.
        """
        log.info("Migrando pagos (account_payment)...")

        src_cols = self.b.get_src_columns('account_payment')
        tgt_cols = self.b.get_tgt_columns('account_payment')

        payments = self.b.fetch_src("SELECT * FROM account_payment ORDER BY id")
        self.b.id_map.setdefault('account_payment', {})

        # Campos comunes entre v12 y v16
        common = {
            'name', 'payment_type', 'partner_type', 'amount', 'currency_id',
            'payment_date', 'communication', 'partner_id', 'journal_id',
            'company_id', 'state', 'payment_method_id',
        }

        inserted = 0
        with self.b.tgt_conn.cursor() as cur:
            for pmt in payments:
                old_id = pmt['id']
                rec = {}

                for fld in common:
                    if fld in src_cols and fld in tgt_cols:
                        rec[fld] = pmt.get(fld)

                rec['company_id'] = self.b.map_company(pmt.get('company_id'))
                rec['create_uid'] = 1
                rec['write_uid'] = 1

                # Mapear FKs
                for fk, ref in [
                    ('partner_id', 'res_partner'),
                    ('journal_id', 'account_journal'),
                    ('currency_id', 'res_currency'),
                    ('destination_journal_id', 'account_journal'),
                ]:
                    if fk in rec and rec[fk]:
                        rec[fk] = self.b.id_map.get(ref, {}).get(rec[fk])

                # Limpiar FK=0
                for f in list(rec.keys()):
                    if f.endswith('_id') and rec[f] == 0:
                        rec[f] = None

                # Odoo 16: date field (antes era payment_date)
                if 'date' in tgt_cols and 'payment_date' in src_cols:
                    rec['date'] = pmt.get('payment_date')

                self.b._fill_not_null(rec, tgt_cols)

                cols_q = ', '.join(f'"{c}"' for c in rec)
                placeholders = ', '.join(['%s'] * len(rec))
                try:
                    cur.execute(
                        f'INSERT INTO account_payment ({cols_q}) VALUES ({placeholders}) RETURNING id',
                        self.b.prepare_vals(rec, tgt_cols)
                    )
                    new_id = cur.fetchone()[0]
                    self.b.id_map['account_payment'][old_id] = new_id
                    inserted += 1
                except Exception as e:
                    self.b.tgt_conn.rollback()
                    log.error("account_payment old_id=%s: %s", old_id, e)

        log.info("account_payment: %d pagos migrados.", inserted)

    # ──────────────────────────────────────────────
    # 8. Post-migración contable
    # ──────────────────────────────────────────────

    def post_migration_updates(self):
        """
        Actualiza referencias circulares y vinculaciones que requieren que
        todos los registros ya existan en destino.
        """
        log.info("Actualizaciones post-migración contable...")
        payment_map = self.b.id_map.get('account_payment', {})
        move_map = self.b.id_map.get('account_move', {})
        aml_map = self.b.id_map.get('account_move_line', {})
        fr_map = self.b.id_map.get('account_full_reconcile', {})

        src_payments = self.b.fetch_src("""
            SELECT id,
                   (SELECT DISTINCT aml.move_id
                    FROM account_move_line aml
                    WHERE aml.payment_id = account_payment.id
                    LIMIT 1) AS linked_move_id
            FROM account_payment
            WHERE id IN (
                SELECT DISTINCT payment_id FROM account_move_line WHERE payment_id IS NOT NULL
            )
        """)

        updated_moves = 0
        with self.b.tgt_conn.cursor() as cur:
            # Vincular account_payment.move_id
            for row in src_payments:
                tgt_pay = payment_map.get(row['id'])
                tgt_move = move_map.get(row['linked_move_id'])
                if tgt_pay and tgt_move:
                    try:
                        cur.execute(
                            "UPDATE account_payment SET move_id=%s WHERE id=%s",
                            (tgt_move, tgt_pay)
                        )
                        updated_moves += 1
                    except Exception as e:
                        self.b.tgt_conn.rollback()
                        log.warning("payment move_id link error: %s", e)

            log.info("Pagos vinculados a moves: %d", updated_moves)

            # Actualizar payment_id en account_move_line
            src_aml_pay = self.b.fetch_src(
                "SELECT id, payment_id FROM account_move_line WHERE payment_id IS NOT NULL"
            )
            updated_aml = 0
            for row in src_aml_pay:
                tgt_aml = aml_map.get(row['id'])
                tgt_pay = payment_map.get(row['payment_id'])
                if tgt_aml and tgt_pay:
                    try:
                        cur.execute(
                            "UPDATE account_move_line SET payment_id=%s WHERE id=%s",
                            (tgt_pay, tgt_aml)
                        )
                        updated_aml += 1
                    except Exception:
                        self.b.tgt_conn.rollback()
            log.info("account_move_line payment_id actualizados: %d", updated_aml)

            # Actualizar full_reconcile_id en account_move_line
            src_aml_fr = self.b.fetch_src(
                "SELECT id, full_reconcile_id FROM account_move_line WHERE full_reconcile_id IS NOT NULL"
            )
            updated_fr = 0
            for row in src_aml_fr:
                tgt_aml = aml_map.get(row['id'])
                tgt_fr = fr_map.get(row['full_reconcile_id'])
                if tgt_aml and tgt_fr:
                    try:
                        cur.execute(
                            "UPDATE account_move_line SET full_reconcile_id=%s WHERE id=%s",
                            (tgt_fr, tgt_aml)
                        )
                        updated_fr += 1
                    except Exception:
                        self.b.tgt_conn.rollback()
            log.info("account_move_line full_reconcile_id actualizados: %d", updated_fr)
