"""
modules/pos.py
Migración de Punto de Venta Odoo 12 -> Odoo 16.

Cambios críticos:
  • pos_config: en Odoo 12 usa journal_ids (M2M) para métodos de pago;
    en Odoo 16 usa pos_payment_method (tabla separada).
    Los payment_method_ids se crean automáticamente si no existen.
  • pos_order: en Odoo 12 los pagos van en account_bank_statement_line;
    en Odoo 16 en pos_payment (tabla separada).
  • pos_session: campos de statement renombrados.
"""

import logging

log = logging.getLogger(__name__)


class PosMigrator:

    def __init__(self, base):
        self.b = base

    def migrate_payment_methods(self):
        """
        Crea pos_payment_method en Odoo 16 a partir de los diarios de POS
        usados en Odoo 12 (pos_config_journal_rel o journal_ids en pos_config).
        """
        log.info("Migrando métodos de pago POS (pos_payment_method)...")

        if not self.b.table_exists_in_tgt('pos_payment_method'):
            log.warning("pos_payment_method no existe en destino.")
            return

        # Obtener diarios usados en POS de Odoo 12
        if self.b.table_exists_in_src('pos_config_journal_rel'):
            journals_src = self.b.fetch_src("""
                SELECT DISTINCT j.id, j.name, j.type, j.company_id
                FROM account_journal j
                JOIN pos_config_journal_rel rel ON rel.journal_id = j.id
            """)
        else:
            # Fallback: todos los diarios de tipo cash/bank en POS
            journals_src = self.b.fetch_src("""
                SELECT DISTINCT j.id, j.name, j.type, j.company_id
                FROM account_journal j
                WHERE j.type IN ('cash', 'bank')
            """)

        tgt_pm_cols = self.b.get_tgt_columns('pos_payment_method')
        self.b.id_map.setdefault('pos_payment_method', {})

        with self.b.tgt_conn.cursor() as cur:
            for j in journals_src:
                new_journal_id = self.b.id_map.get('account_journal', {}).get(j['id'])
                if not new_journal_id:
                    continue

                new_company_id = self.b.map_company(j['company_id'])
                is_cash = j['type'] == 'cash'

                rec = {
                    'name': j['name'],
                    'is_cash_count': is_cash,
                    'company_id': new_company_id,
                    'journal_id': new_journal_id,
                    'create_uid': 1,
                    'write_uid': 1,
                }

                # Filtrar solo columnas que existen
                rec = {k: v for k, v in rec.items() if k in tgt_pm_cols}

                cols_q = ', '.join(f'"{c}"' for c in rec)
                placeholders = ', '.join(['%s'] * len(rec))
                try:
                    cur.execute(
                        f'INSERT INTO pos_payment_method ({cols_q}) VALUES ({placeholders}) RETURNING id',
                        self.b.prepare_vals(rec, tgt_pm_cols)
                    )
                    new_pm_id = cur.fetchone()[0]
                    # Mapear journal_id -> payment_method_id para los pagos de órdenes
                    self.b.id_map['pos_payment_method'][j['id']] = new_pm_id
                    log.debug("pos_payment_method creado: %s (journal_id=%s)", new_pm_id, j['id'])
                except Exception as e:
                    self.b.tgt_conn.rollback()
                    # Intentar recuperar existente
                    try:
                        cur.execute(
                            'SELECT id FROM pos_payment_method WHERE journal_id=%s AND company_id=%s LIMIT 1',
                            (new_journal_id, new_company_id)
                        )
                        res = cur.fetchone()
                        if res:
                            self.b.id_map['pos_payment_method'][j['id']] = res[0]
                    except Exception:
                        self.b.tgt_conn.rollback()
                    log.debug("pos_payment_method ya existe para journal %s: %s", j['id'], e)

        log.info("pos_payment_method: %d métodos procesados.", len(journals_src))

    def migrate_config(self):
        """Migra pos_config."""
        log.info("Migrando configuración POS (pos_config)...")

        skip_fields = [
            'journal_ids',            # -> payment_method_ids en Odoo 16
            'journal_id',             # Diario principal, manejado por account_journal
            'message_main_attachment_id',
        ]

        self.b.migrate_table(
            'pos_config',
            mapping_fields={
                'picking_type_id': 'stock_picking_type',
                'company_id': None,   # Manejado por map_company
                'default_partner_id': 'res_partner',
                'invoice_journal_id': 'account_journal',
                'sequence_id': 'ir_sequence',
                'sequence_line_id': 'ir_sequence',
                'warehouse_id': 'stock_warehouse',
            },
            skip_fields=skip_fields,
        )

        # Vincular payment_method_ids a pos_config (M2M)
        self._link_payment_methods()

    def _link_payment_methods(self):
        """
        Crea la relación M2M entre pos_config y pos_payment_method.
        En Odoo 12: pos_config_journal_rel (config_id, journal_id)
        En Odoo 16: pos_config_pos_payment_method_rel o payment_method_ids
        """
        rel_table_tgt = None
        for candidate in ('pos_config_pos_payment_method_rel', 'pos_payment_method_pos_config_rel'):
            if self.b.table_exists_in_tgt(candidate):
                rel_table_tgt = candidate
                break

        if not rel_table_tgt:
            log.warning("No se encontró tabla relacional para pos_config <-> pos_payment_method.")
            return

        if not self.b.table_exists_in_src('pos_config_journal_rel'):
            log.warning("pos_config_journal_rel no encontrada en origen.")
            return

        # Detectar el nombre real de la columna de config en la tabla origen
        src_cols = self.b.get_src_columns('pos_config_journal_rel')
        config_col = 'pos_config_id' if 'pos_config_id' in src_cols else 'config_id'
        relations = self.b.fetch_src(
            f"SELECT {config_col} AS config_id, journal_id FROM pos_config_journal_rel"
        )

        inserted = 0
        with self.b.tgt_conn.cursor() as cur:
            for rel in relations:
                new_config = self.b.id_map.get('pos_config', {}).get(rel['config_id'])
                new_pm = self.b.id_map.get('pos_payment_method', {}).get(rel['journal_id'])
                if not new_config or not new_pm:
                    continue
                try:
                    cur.execute(
                        f'INSERT INTO {rel_table_tgt} (pos_config_id, payment_method_id) '
                        f'VALUES (%s, %s) ON CONFLICT DO NOTHING',
                        (new_config, new_pm)
                    )
                    inserted += 1
                except Exception as e:
                    self.b.tgt_conn.rollback()
                    log.debug("M2M pos_config<->payment_method: %s", e)

        log.info("pos_config <-> pos_payment_method: %d relaciones vinculadas.", inserted)

    def migrate_sessions(self):
        """Migra pos_session."""
        log.info("Migrando sesiones POS (pos_session)...")

        src_cols = self.b.get_src_columns('pos_session')
        tgt_cols = self.b.get_tgt_columns('pos_session')

        # Campos a omitir si son específicos de v12
        skip = {
            'statement_ids',           # Bank statements, no aplica en v16
            'message_main_attachment_id',
        }

        cols_copy = [c for c in tgt_cols if c != 'id' and c in src_cols and c not in skip]

        sessions = self.b.fetch_src("SELECT * FROM pos_session ORDER BY id")
        self.b.id_map.setdefault('pos_session', {})

        inserted = 0
        with self.b.tgt_conn.cursor() as cur:
            for s in sessions:
                old_id = s['id']
                rec = {c: s[c] for c in cols_copy if c in s}

                rec['company_id'] = self.b.map_company(s.get('company_id'))
                rec['create_uid'] = 1
                rec['write_uid'] = 1

                for fk, ref in [
                    ('config_id', 'pos_config'),
                    ('user_id', None),  # res_users - usar admin
                    ('cash_journal_id', 'account_journal'),
                    ('cash_control_difference_id', 'account_account'),
                    ('sequence_number', None),
                ]:
                    if fk in rec:
                        if ref and rec[fk]:
                            rec[fk] = self.b.id_map.get(ref, {}).get(rec[fk])
                        elif fk == 'user_id':
                            rec[fk] = 1  # Admin

                for f in list(rec.keys()):
                    if f.endswith('_id') and rec.get(f) == 0:
                        rec[f] = None

                self.b._fill_not_null(rec, tgt_cols)

                cols_q = ', '.join(f'"{c}"' for c in rec)
                placeholders = ', '.join(['%s'] * len(rec))
                try:
                    cur.execute(
                        f'INSERT INTO pos_session ({cols_q}) VALUES ({placeholders}) RETURNING id',
                        self.b.prepare_vals(rec, tgt_cols)
                    )
                    new_id = cur.fetchone()[0]
                    self.b.id_map['pos_session'][old_id] = new_id
                    inserted += 1
                except Exception as e:
                    self.b.tgt_conn.rollback()
                    log.error("pos_session old_id=%s: %s", old_id, e)

        log.info("pos_session: %d sesiones migradas.", inserted)

    def migrate_orders(self):
        """Migra pos_order."""
        log.info("Migrando órdenes POS (pos_order)...")
        self.b.migrate_table(
            'pos_order',
            mapping_fields={
                'partner_id': 'res_partner',
                'session_id': 'pos_session',
                'config_id': 'pos_config',
                'picking_id': 'stock_picking',
                'invoice_id': 'account_move',    # account_invoice en v12 -> account_move en v16
                'currency_id': 'res_currency',
                'fiscal_position_id': 'account_fiscal_position',
                'sale_journal': 'account_journal',
            },
            skip_fields=[
                'statement_ids',
                'message_main_attachment_id',
            ],
        )

    def migrate_order_lines(self):
        """Migra pos_order_line."""
        log.info("Migrando líneas de órdenes POS (pos_order_line)...")
        self.b.migrate_table(
            'pos_order_line',
            mapping_fields={
                'order_id': 'pos_order',
                'product_id': 'product_product',
                'pack_lot_ids': None,    # Se maneja aparte
            },
            skip_fields=['pack_lot_ids'],
        )
        # Impuestos en líneas POS (M2M)
        self.b.migrate_m2m(
            'account_tax_pos_order_line_rel',
            'pos_order_line_id', 'account_tax_id',
            'pos_order_line', 'account_tax'
        )

    def migrate_pos_payments(self):
        """
        En Odoo 12, los pagos POS están en account_bank_statement_line.
        En Odoo 16, están en pos_payment.
        Migra creando registros en pos_payment a partir de los datos disponibles.
        """
        log.info("Migrando pagos POS (pos_payment)...")

        if not self.b.table_exists_in_tgt('pos_payment'):
            log.warning("pos_payment no existe en destino.")
            return

        # En Odoo 12, los pagos POS se registran en account_bank_statement_line
        # vinculados a pos_order via pos_order.statement_ids (M2M)
        if not self.b.table_exists_in_src('account_bank_statement_line'):
            log.warning("account_bank_statement_line no existe en origen, saltando pagos POS.")
            return

        src_has_rel = self.b.table_exists_in_src('pos_order_account_bank_statement_line_rel') or \
                      self.b.table_exists_in_src('pos_order_statement_ids_rel')

        if src_has_rel:
            rel_table = ('pos_order_account_bank_statement_line_rel'
                         if self.b.table_exists_in_src('pos_order_account_bank_statement_line_rel')
                         else 'pos_order_statement_ids_rel')
            payments = self.b.fetch_src(f"""
                SELECT
                    bsl.id          AS old_id,
                    rel.pos_order_id AS order_id,
                    bsl.amount,
                    bsl.date,
                    bsl.name,
                    bs.journal_id
                FROM account_bank_statement_line bsl
                JOIN {rel_table} rel ON rel.statement_line_id = bsl.id
                JOIN account_bank_statement bs ON bs.id = bsl.statement_id
                ORDER BY bsl.id
            """)
        else:
            # Fallback: buscar por pos_order_id si existe el campo
            src_bsl_cols = self.b.get_src_columns('account_bank_statement_line')
            if 'pos_order_id' not in src_bsl_cols:
                log.warning("No se puede vincular pagos POS con órdenes. Saltando.")
                return
            payments = self.b.fetch_src("""
                SELECT
                    bsl.id          AS old_id,
                    bsl.pos_order_id AS order_id,
                    bsl.amount,
                    bsl.date,
                    bsl.name,
                    bs.journal_id
                FROM account_bank_statement_line bsl
                JOIN account_bank_statement bs ON bs.id = bsl.statement_id
                WHERE bsl.pos_order_id IS NOT NULL
                ORDER BY bsl.id
            """)

        tgt_pay_cols = self.b.get_tgt_columns('pos_payment')
        inserted = 0

        with self.b.tgt_conn.cursor() as cur:
            for pmt in payments:
                new_order_id = self.b.id_map.get('pos_order', {}).get(pmt['order_id'])
                new_pm_id = self.b.id_map.get('pos_payment_method', {}).get(pmt['journal_id'])
                if not new_order_id:
                    continue

                rec = {
                    'pos_order_id': new_order_id,
                    'payment_method_id': new_pm_id,
                    'amount': float(pmt['amount'] or 0),
                    'payment_date': pmt['date'],
                    'name': pmt['name'] or '',
                    'create_uid': 1,
                    'write_uid': 1,
                }

                # Obtener session_id desde la orden
                try:
                    cur.execute(
                        'SELECT session_id, company_id FROM pos_order WHERE id=%s',
                        (new_order_id,)
                    )
                    ord_row = cur.fetchone()
                    if ord_row:
                        rec['session_id'] = ord_row[0]
                        rec['company_id'] = ord_row[1]
                except Exception:
                    self.b.tgt_conn.rollback()

                rec = {k: v for k, v in rec.items() if k in tgt_pay_cols}

                cols_q = ', '.join(f'"{c}"' for c in rec)
                placeholders = ', '.join(['%s'] * len(rec))
                try:
                    cur.execute(
                        f'INSERT INTO pos_payment ({cols_q}) VALUES ({placeholders})',
                        self.b.prepare_vals(rec, tgt_pay_cols)
                    )
                    inserted += 1
                except Exception as e:
                    self.b.tgt_conn.rollback()
                    log.debug("pos_payment insert error: %s", e)

        log.info("pos_payment: %d pagos POS migrados.", inserted)
