"""
modules/method_minori.py
Migración de los modelos del módulo method_minori (Odoo 12 -> Odoo 16).

Tablas manejadas:
  • method_minori_marcas     - marcas de productos
  • method_minori_periodos   - períodos de comisión
  • product_template.marca_id - backfill del vínculo producto -> marca

Notas:
  - Ni marcas ni periodos tienen company_id (son agnósticas de empresa), por lo que
    NO se filtran/mapean por empresa.
  - En el destino (Odoo 16) marcas.partner_id es requerido en el modelo pero no existe
    en el origen: se asigna el partner de la empresa destino (DEFAULT_TARGET_COMPANY_ID).
  - marcas.user_id es FK a res_users; los usuarios no se migran, así que se conserva el
    mismo uid si existe en destino o se usa admin (uid=1) como fallback.
  - product_template.marca_id se rellena SOLO donde esté NULL y acotado a la empresa
    destino (company_id = DEFAULT_TARGET_COMPANY_ID).
"""

import logging

import config as cfg

log = logging.getLogger(__name__)


class MethodMinoriMigrator:

    def __init__(self, base):
        """
        Args:
            base: instancia de BaseMigrator (acceso a src_conn, tgt_conn, id_map, etc.)
        """
        self.b = base

    # ──────────────────────────────────────────────
    # Orquestación
    # ──────────────────────────────────────────────

    def migrate_all(self):
        self.migrate_marcas()
        self.migrate_periodos()
        self.backfill_product_marca()

    # ──────────────────────────────────────────────
    # Helpers internos
    # ──────────────────────────────────────────────

    def _company_partner_id(self):
        """Retorna el partner_id de la empresa destino (DEFAULT_TARGET_COMPANY_ID)."""
        with self.b.tgt_conn.cursor() as cur:
            cur.execute(
                "SELECT partner_id FROM res_company WHERE id=%s",
                (cfg.DEFAULT_TARGET_COMPANY_ID,),
            )
            row = cur.fetchone()
        return row[0] if row and row[0] else None

    # ──────────────────────────────────────────────
    # Marcas
    # ──────────────────────────────────────────────

    def migrate_marcas(self):
        """Migra method_minori_marcas. Dedup por nombre; sin company_id."""
        log.info("=== Migrando method_minori_marcas ===")
        table = 'method_minori_marcas'

        if not self.b.table_exists_in_src(table):
            log.warning("Tabla %s no existe en origen, saltando.", table)
            return
        if not self.b.table_exists_in_tgt(table):
            log.warning("Tabla %s no existe en destino, saltando.", table)
            return

        # res_users: mapear mismo id si existe en destino (fallback admin=1)
        self.b.preload_id_map('res_users')

        company_partner_id = self._company_partner_id()
        if not company_partner_id:
            log.warning("No se encontró partner_id para la empresa %s; "
                        "marcas quedarán con partner_id NULL.",
                        cfg.DEFAULT_TARGET_COMPANY_ID)

        src_cols = self.b.get_src_columns(table)
        tgt_cols = self.b.get_tgt_columns(table)

        # Columnas comunes de datos (excluyendo id, uid/date de sistema y FKs manejadas aparte)
        skip = {'id', 'user_id', 'partner_id',
                'create_uid', 'write_uid', 'create_date', 'write_date',
                'message_main_attachment_id'}
        common_cols = [c for c in src_cols if c in tgt_cols and c not in skip]

        # Dedup: {lower(name): id} de las marcas ya existentes en destino
        with self.b.tgt_conn.cursor() as cur:
            cur.execute("SELECT id, name FROM method_minori_marcas")
            existing = {}
            for tid, name in cur.fetchall():
                if name:
                    existing.setdefault(name.strip().lower(), tid)

        rows = self.b.fetch_src(f'SELECT * FROM "{table}" ORDER BY id')
        self.b.id_map.setdefault(table, {})

        inserted = 0
        mapped = 0
        with self.b.tgt_conn.cursor() as cur:
            for row in rows:
                old_id = row['id']
                name = (row.get('name') or '').strip()

                # Dedup por nombre
                key = name.lower()
                if key and key in existing:
                    self.b.id_map[table][old_id] = existing[key]
                    mapped += 1
                    continue

                rec = {c: row[c] for c in common_cols}

                # user_id: mismo uid si existe en destino, si no admin
                old_uid = row.get('user_id')
                rec['user_id'] = self.b.id_map.get('res_users', {}).get(old_uid, 1) or 1

                # partner_id: partner de la empresa destino (requerido en el modelo v16)
                if 'partner_id' in tgt_cols:
                    rec['partner_id'] = company_partner_id

                # company_id: aunque no existe en origen, en destino debe quedar
                # marcada como de la empresa destino (evita que quede en NULL,
                # visible/compartida entre todas las empresas).
                if 'company_id' in tgt_cols:
                    rec['company_id'] = cfg.DEFAULT_TARGET_COMPANY_ID

                rec['create_uid'] = 1
                rec['write_uid'] = 1

                self.b._fill_not_null(rec, tgt_cols)

                cols_q = ', '.join(f'"{c}"' for c in rec)
                placeholders = ', '.join(['%s'] * len(rec))
                try:
                    cur.execute(
                        f'INSERT INTO "{table}" ({cols_q}) VALUES ({placeholders}) RETURNING id',
                        self.b.prepare_vals(rec, tgt_cols),
                    )
                    new_id = cur.fetchone()[0]
                    self.b.id_map[table][old_id] = new_id
                    if key:
                        existing[key] = new_id
                    inserted += 1
                except Exception as e:
                    self.b.tgt_conn.rollback()
                    log.error("%s old_id=%s: %s", table, old_id, e)

        log.info("%-30s insertados=%-4d mapeados(existentes)=%d", table, inserted, mapped)

    # ──────────────────────────────────────────────
    # Períodos
    # ──────────────────────────────────────────────

    def migrate_periodos(self):
        """Migra method_minori_periodos. Dedup por nombre; sin company_id."""
        log.info("=== Migrando method_minori_periodos ===")
        table = 'method_minori_periodos'

        if not self.b.table_exists_in_src(table):
            log.warning("Tabla %s no existe en origen, saltando.", table)
            return
        if not self.b.table_exists_in_tgt(table):
            log.warning("Tabla %s no existe en destino, saltando.", table)
            return

        src_cols = self.b.get_src_columns(table)
        tgt_cols = self.b.get_tgt_columns(table)

        skip = {'id', 'create_uid', 'write_uid', 'create_date', 'write_date',
                'message_main_attachment_id'}
        common_cols = [c for c in src_cols if c in tgt_cols and c not in skip]

        with self.b.tgt_conn.cursor() as cur:
            cur.execute("SELECT id, name FROM method_minori_periodos")
            existing = {}
            for tid, name in cur.fetchall():
                if name:
                    existing.setdefault(name.strip().lower(), tid)

        rows = self.b.fetch_src(f'SELECT * FROM "{table}" ORDER BY id')
        self.b.id_map.setdefault(table, {})

        inserted = 0
        mapped = 0
        with self.b.tgt_conn.cursor() as cur:
            for row in rows:
                old_id = row['id']
                name = (row.get('name') or '').strip()
                key = name.lower()
                if key and key in existing:
                    self.b.id_map[table][old_id] = existing[key]
                    mapped += 1
                    continue

                rec = {c: row[c] for c in common_cols}

                # company_id: aunque no existe en origen, en destino debe quedar
                # marcada como de la empresa destino (evita que quede en NULL).
                if 'company_id' in tgt_cols:
                    rec['company_id'] = cfg.DEFAULT_TARGET_COMPANY_ID

                rec['create_uid'] = 1
                rec['write_uid'] = 1

                self.b._fill_not_null(rec, tgt_cols)

                cols_q = ', '.join(f'"{c}"' for c in rec)
                placeholders = ', '.join(['%s'] * len(rec))
                try:
                    cur.execute(
                        f'INSERT INTO "{table}" ({cols_q}) VALUES ({placeholders}) RETURNING id',
                        self.b.prepare_vals(rec, tgt_cols),
                    )
                    new_id = cur.fetchone()[0]
                    self.b.id_map[table][old_id] = new_id
                    if key:
                        existing[key] = new_id
                    inserted += 1
                except Exception as e:
                    self.b.tgt_conn.rollback()
                    log.error("%s old_id=%s: %s", table, old_id, e)

        log.info("%-30s insertados=%-4d mapeados(existentes)=%d", table, inserted, mapped)

    # ──────────────────────────────────────────────
    # Backfill product_template.marca_id
    # ──────────────────────────────────────────────

    def backfill_product_marca(self):
        """
        Vincula productos migrados con su marca.
        Solo actualiza product_template.marca_id donde esté NULL y acotado a la
        empresa destino (company_id = DEFAULT_TARGET_COMPANY_ID).
        """
        log.info("=== Backfill product_template.marca_id ===")
        target_company_id = cfg.DEFAULT_TARGET_COMPANY_ID

        if 'marca_id' not in self.b.get_src_columns('product_template'):
            log.warning("product_template.marca_id no existe en origen, saltando backfill.")
            return
        if 'marca_id' not in self.b.get_tgt_columns('product_template'):
            log.warning("product_template.marca_id no existe en destino, saltando backfill.")
            return

        tmpl_map = self.b.id_map.get('product_template', {})
        marca_map = self.b.id_map.get('method_minori_marcas', {})
        if not tmpl_map:
            log.warning("id_map de product_template vacío; ¿se migraron productos antes? "
                        "Saltando backfill.")
            return
        if not marca_map:
            log.warning("id_map de method_minori_marcas vacío; ¿se migraron marcas antes? "
                        "Saltando backfill.")
            return

        rows = self.b.fetch_src(
            "SELECT id, marca_id FROM product_template WHERE marca_id IS NOT NULL"
        )

        updated = 0
        skipped = 0
        with self.b.tgt_conn.cursor() as cur:
            for row in rows:
                new_tmpl = tmpl_map.get(row['id'])
                new_marca = marca_map.get(row['marca_id'])
                if not new_tmpl or not new_marca:
                    skipped += 1
                    continue
                cur.execute(
                    "UPDATE product_template SET marca_id=%s "
                    "WHERE id=%s AND marca_id IS NULL AND company_id=%s",
                    (new_marca, new_tmpl, target_company_id),
                )
                updated += cur.rowcount
        self.b.tgt_conn.commit()

        log.info("product_template.marca_id: %d actualizados, %d sin mapeo (tmpl/marca).",
                 updated, skipped)
