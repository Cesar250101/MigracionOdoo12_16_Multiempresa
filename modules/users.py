"""
modules/users.py
Migración de res.users Odoo 12 -> Odoo 16.

Alcance deliberadamente acotado (decisión del usuario):
  - Se migran solo usuarios internos reales (se excluyen los logins técnicos
    propios de Odoo: __system__, default, public, portaltemplate).
  - Se excluyen además los ids listados en cfg.SKIP_SOURCE_USER_IDS (ej. el
    administrador, que ya existe en destino con el mismo login).
  - Dedup por login (case-insensitive): si ya existe un usuario con ese login
    en destino, se reutiliza su id en vez de crear uno nuevo.
  - Los usuarios nuevos quedan solo con el grupo "Internal User" (acceso
    básico para iniciar sesión); no se replican los permisos/; grupos de
    origen porque los ids de grupo no corresponden 1:1 entre Odoo 12 y 16.
  - No se migran action_id/sale_team_id/website_id/alias_id (FKs a modelos
    no mapeados en esta migración): quedan en su valor por defecto.
"""

import logging

import config as cfg

log = logging.getLogger(__name__)

SKIP_LOGINS = {'__system__', 'default', 'public', 'portaltemplate'}


class UsersMigrator:

    def __init__(self, base):
        self.b = base

    def migrate_users(self):
        """Migra res_users (usuarios internos reales, ver docstring del módulo)."""
        log.info("=== Migrando usuarios (res_users) ===")
        table = 'res_users'

        if not self.b.table_exists_in_src(table) or not self.b.table_exists_in_tgt(table):
            log.warning("%s no existe en origen o destino, saltando.", table)
            return

        group_user_id = self._get_group_user_id()
        if not group_user_id:
            log.warning("No se encontró el grupo base.group_user en destino; "
                        "los usuarios migrados quedarán sin grupos.")

        src_cols = self.b.get_src_columns(table)
        tgt_cols = self.b.get_tgt_columns(table)

        # Campos seguros a copiar tal cual (excluye FKs no mapeadas en esta
        # migración: action_id, sale_team_id, website_id, alias_id, company_id,
        # partner_id, create_uid/write_uid, y campos manejados aparte).
        safe_fields = ['active', 'login', 'password', 'signature',
                        'create_date', 'write_date']
        common_cols = [c for c in safe_fields if c in src_cols and c in tgt_cols]

        skip_ids = set(cfg.SKIP_SOURCE_USER_IDS or [])

        rows = self.b.fetch_src(f'SELECT * FROM "{table}" ORDER BY id')
        self.b.id_map.setdefault(table, {})

        # Dedup: {lower(login): id} de usuarios ya existentes en destino
        with self.b.tgt_conn.cursor() as cur:
            cur.execute("SELECT id, login FROM res_users")
            existing = {login.strip().lower(): tid for tid, login in cur.fetchall() if login}

        inserted = 0
        mapped = 0
        skipped_system = 0
        with self.b.tgt_conn.cursor() as cur:
            for row in rows:
                old_id = row['id']
                login = (row.get('login') or '').strip()

                if old_id in skip_ids or login.lower() in SKIP_LOGINS:
                    skipped_system += 1
                    continue

                key = login.lower()
                if key and key in existing:
                    self.b.id_map[table][old_id] = existing[key]
                    mapped += 1
                    continue

                rec = {c: row[c] for c in common_cols}

                new_partner_id = self.b.id_map.get('res_partner', {}).get(row.get('partner_id'))
                if not new_partner_id:
                    log.warning("res_users old_id=%s (%s): partner_id no mapeado, saltando.",
                                old_id, login)
                    continue
                rec['partner_id'] = new_partner_id

                rec['company_id'] = cfg.DEFAULT_TARGET_COMPANY_ID
                rec['share'] = False
                if 'notification_type' in tgt_cols:
                    rec['notification_type'] = 'email'
                if 'sidebar_type' in tgt_cols:
                    rec['sidebar_type'] = 'large'

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

                    # company_ids (M2M res_company_users_rel)
                    cur.execute(
                        'INSERT INTO res_company_users_rel (cid, user_id) '
                        'VALUES (%s, %s) ON CONFLICT DO NOTHING',
                        (cfg.DEFAULT_TARGET_COMPANY_ID, new_id),
                    )

                    # Grupo base: Internal User (acceso mínimo para iniciar sesión)
                    if group_user_id:
                        cur.execute(
                            'INSERT INTO res_groups_users_rel (gid, uid) '
                            'VALUES (%s, %s) ON CONFLICT DO NOTHING',
                            (group_user_id, new_id),
                        )
                except Exception as e:
                    self.b.tgt_conn.rollback()
                    log.error("res_users old_id=%s (%s): %s", old_id, login, e)

        log.info("%-30s insertados=%-4d mapeados(existentes)=%-4d omitidos(sistema)=%d",
                  table, inserted, mapped, skipped_system)

    def _get_group_user_id(self):
        """Busca el id destino de base.group_user (Internal User)."""
        with self.b.tgt_conn.cursor() as cur:
            cur.execute(
                "SELECT res_id FROM ir_model_data "
                "WHERE module='base' AND name='group_user' LIMIT 1"
            )
            row = cur.fetchone()
        return row[0] if row else None
