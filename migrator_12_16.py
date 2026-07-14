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
from modules.repair import RepairMigrator
from modules.method_minori import MethodMinoriMigrator
from modules.users import UsersMigrator

log = logging.getLogger(__name__)


class Migrator12to16:

    def __init__(self, source_db: dict = None, target_db: dict = None,
                 company_migration: list = None):
        source_db = source_db or cfg.SOURCE_DB
        target_db = target_db or cfg.TARGET_DB
        company_list = company_migration or cfg.COMPANY_MIGRATION

        log.info("Conectando a BD origen (Odoo 12): %s@%s/%s",
                 source_db['user'], source_db['host'], source_db['dbname'])
        # keepalives evitan que el firewall/NAT cierre la conexión remota inactiva
        self.src_conn = psycopg2.connect(
            **source_db,
            keepalives=1,
            keepalives_idle=60,
            keepalives_interval=10,
            keepalives_count=5,
        )

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
        self.pos = PosMigrator(self.base, self.accounting)
        self.repair = RepairMigrator(self.base)
        self.method_minori = MethodMinoriMigrator(self.base)
        self.users = UsersMigrator(self.base)

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
    # Normalización previa de company_id
    # ──────────────────────────────────────────────

    def normalize_existing_company_id(self):
        """
        Antes de migrar, fuerza company_id = EXISTING_DATA_COMPANY_ID en TODAS
        las filas de destino que ya existían (en cualquier tabla que tenga esa
        columna), para separar limpiamente "lo que ya existía" (empresa
        EXISTING_DATA_COMPANY_ID) de "lo que se migra desde Odoo 12" (empresa
        DEFAULT_TARGET_COMPANY_ID).

        Protege las filas que ya pertenecen a DEFAULT_TARGET_COMPANY_ID para
        que un rerun de la migración completa no le quite datos ya migrados.
        """
        log.info("=== Normalizando company_id de datos preexistentes ===")

        with self.tgt_conn.cursor() as cur:
            cur.execute("""
                SELECT c.table_name
                FROM information_schema.columns c
                JOIN information_schema.tables t
                  ON t.table_schema = c.table_schema AND t.table_name = c.table_name
                WHERE c.column_name = 'company_id'
                  AND c.table_schema = 'public'
                  AND t.table_type = 'BASE TABLE'
                ORDER BY c.table_name
            """)
            tables = [row[0] for row in cur.fetchall()]

        keep_null_tables = getattr(cfg, 'NORMALIZE_KEEP_NULL_TABLES', set())

        updated_tables = 0
        total_rows = 0
        with self.tgt_conn.cursor() as cur:
            for table in tables:
                try:
                    if table in keep_null_tables:
                        # company_id=NULL es legítimo aquí (registro compartido
                        # entre compañías): no lo tocamos, solo normalizamos
                        # valores "sucios" que no sean NULL/1/11.
                        cur.execute(
                            f'UPDATE "{table}" SET company_id = %s '
                            f'WHERE company_id IS NOT NULL '
                            f'  AND company_id IS DISTINCT FROM %s '
                            f'  AND company_id IS DISTINCT FROM %s',
                            (cfg.EXISTING_DATA_COMPANY_ID,
                             cfg.EXISTING_DATA_COMPANY_ID,
                             cfg.DEFAULT_TARGET_COMPANY_ID),
                        )
                    else:
                        cur.execute(
                            f'UPDATE "{table}" SET company_id = %s '
                            f'WHERE company_id IS DISTINCT FROM %s '
                            f'  AND company_id IS DISTINCT FROM %s',
                            (cfg.EXISTING_DATA_COMPANY_ID,
                             cfg.EXISTING_DATA_COMPANY_ID,
                             cfg.DEFAULT_TARGET_COMPANY_ID),
                        )
                    if cur.rowcount:
                        updated_tables += 1
                        total_rows += cur.rowcount
                        log.debug("%s: %d filas -> company_id=%s",
                                  table, cur.rowcount, cfg.EXISTING_DATA_COMPANY_ID)
                except Exception as e:
                    self.tgt_conn.rollback()
                    log.warning("No se pudo normalizar company_id en %s: %s",
                                table, str(e).split('\n')[0])

        log.info("Normalización company_id: %d tablas modificadas, %d filas actualizadas a company_id=%s.",
                  updated_tables, total_rows, cfg.EXISTING_DATA_COMPANY_ID)

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

        # Tablas con limpieza especial (DELETEs seguros con subqueries de FK)
        SAFE_DELETE_TABLES = {'product_product', 'product_template', 'res_partner'}

        with self.tgt_conn.cursor() as cur:
            # --- Limpiar tablas sin company_id que bloquearían account_account ---
            # account_fiscal_position_account/tax referencian account_account y
            # account_tax pero no tienen company_id almacenado en BD (es related).
            placeholders = ', '.join(['%s'] * len(company_ids))
            for fpa_table in ('account_fiscal_position_account', 'account_fiscal_position_tax'):
                try:
                    cur.execute(
                        f'DELETE FROM "{fpa_table}" WHERE position_id IN '
                        f'(SELECT id FROM account_fiscal_position '
                        f' WHERE company_id IN ({placeholders}))',
                        list(company_ids)
                    )
                    log.debug("Limpiado: %s", fpa_table)
                except Exception as e:
                    self.tgt_conn.rollback()
                    log.warning("No se pudo limpiar %s: %s", fpa_table, str(e).split('\n')[0])

            # --- Limpiar account_payment (tampoco tiene company_id propio; se
            # deriva de move_id -> account_move.company_id). Debe ir antes de
            # que el loop genérico borre account_move, si no queda huérfano.
            try:
                cur.execute(
                    f'DELETE FROM account_payment WHERE move_id IN '
                    f'(SELECT id FROM account_move WHERE company_id IN ({placeholders}))',
                    list(company_ids)
                )
                log.debug("Limpiado: account_payment (%d filas)", cur.rowcount)
            except Exception as e:
                self.tgt_conn.rollback()
                log.warning("No se pudo limpiar account_payment: %s", str(e).split('\n')[0])

            # --- Limpieza genérica (todas las tablas excepto las especiales) ---
            for table in cfg.TABLES_TO_CLEAN:
                if table in SAFE_DELETE_TABLES:
                    continue
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

                    # pos_session no tiene company_id propio (se deriva de
                    # config_id -> pos_config.company_id), así que el chequeo
                    # de columna de arriba la saltearía siempre. Se limpia
                    # aquí mismo, justo después de pos_order (ya sin filas
                    # que la referencien) y antes de que el loop llegue a
                    # pos_config (que la subquery todavía necesita viva).
                    # Sin esto, las sesiones quedan huérfanas entre corridas
                    # y chocan por "pos_session_uniq_name" en el rerun.
                    if table == 'pos_order':
                        try:
                            cur.execute(
                                f'DELETE FROM pos_session WHERE config_id IN '
                                f'(SELECT id FROM pos_config WHERE company_id IN ({placeholders}))',
                                list(company_ids)
                            )
                            log.debug("Limpiado: pos_session (%d filas)", cur.rowcount)
                        except Exception as e:
                            self.tgt_conn.rollback()
                            log.warning("No se pudo limpiar pos_session: %s", str(e).split('\n')[0])
                except Exception as e:
                    self.tgt_conn.rollback()
                    log.warning("No se pudo limpiar %s: %s", table, str(e).split('\n')[0])

            # --- Limpieza segura de product_product, product_template y res_partner ---
            # Se usan DELETEs con subqueries para respetar FKs activas que no pudieron
            # limpiarse (ej: res_company.partner_id impide borrar partner de la empresa).
            for cid in company_ids:
                # product_product primero (FK product_tmpl_id -> product_template)
                try:
                    cur.execute("""
                        DELETE FROM product_product
                        WHERE company_id = %s
                          AND id NOT IN (SELECT product_id FROM account_move_line   WHERE COALESCE(product_id,0)!=0)
                          AND id NOT IN (SELECT product_id FROM sale_order_line     WHERE COALESCE(product_id,0)!=0)
                          AND id NOT IN (SELECT product_id FROM purchase_order_line WHERE COALESCE(product_id,0)!=0)
                          AND id NOT IN (SELECT product_id FROM stock_move          WHERE COALESCE(product_id,0)!=0)
                    """, (cid,))
                    log.debug("product_product limpiado (company_id=%s).", cid)
                except Exception as e:
                    self.tgt_conn.rollback()
                    log.warning("No se pudo limpiar product_product (company %s): %s", cid, str(e).split('\n')[0])

                # product_template después (requiere product_product ya limpiado)
                try:
                    cur.execute("""
                        DELETE FROM product_template
                        WHERE company_id = %s
                          AND id NOT IN (SELECT product_tmpl_id FROM product_product WHERE COALESCE(product_tmpl_id,0)!=0)
                    """, (cid,))
                    log.debug("product_template limpiado (company_id=%s).", cid)
                except Exception as e:
                    self.tgt_conn.rollback()
                    log.warning("No se pudo limpiar product_template (company %s): %s", cid, str(e).split('\n')[0])

                # res_partner: excluir los que aún son referenciados por tablas del sistema
                try:
                    cur.execute("""
                        DELETE FROM res_partner
                        WHERE company_id = %s
                          AND id NOT IN (SELECT partner_id           FROM account_move    WHERE COALESCE(partner_id,0)!=0)
                          AND id NOT IN (SELECT commercial_partner_id FROM account_move   WHERE COALESCE(commercial_partner_id,0)!=0)
                          AND id NOT IN (SELECT partner_id           FROM res_users       WHERE COALESCE(partner_id,0)!=0)
                          AND id NOT IN (SELECT partner_id           FROM purchase_order  WHERE COALESCE(partner_id,0)!=0)
                          AND id NOT IN (SELECT partner_id           FROM sale_order      WHERE COALESCE(partner_id,0)!=0)
                          AND id NOT IN (SELECT partner_id           FROM res_company     WHERE partner_id IS NOT NULL)
                    """, (cid,))
                    log.debug("res_partner limpiado (company_id=%s).", cid)
                except Exception as e:
                    self.tgt_conn.rollback()
                    log.warning("No se pudo limpiar res_partner (company %s): %s", cid, str(e).split('\n')[0])

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
        self._map_sii_activity_description()

        src_cols = self.base.get_src_columns('res_partner')
        mapping = {}

        ref_fks = {
            'country_id':           'res_country',
            'state_id':             'res_country_state',
            'city_id':              'res_city',
            'title':                'res_partner_title',
            'activity_description': 'sii_activity_description',
            # user_id (vendedor asignado): los usuarios no se migran; se conserva
            # el mismo uid solo si existe en destino (preload_id_map), si no NULL.
            'user_id':              'res_users',
        }
        for fk, ref in ref_fks.items():
            if fk in src_cols:
                # res_partner_title / res_users: preload por ID directo (suele coincidir)
                if ref in ('res_partner_title', 'res_users'):
                    self.base.preload_id_map(ref)
                mapping[fk] = ref

        # tz: algunos nombres de huso horario "legacy" (alias retro-compatibles
        # de la tzdata) no son reconocidos por el pytz del destino y rompen la
        # ficha del contacto/usuario (ValueError al leer res.users.tz). Se
        # normalizan a su nombre IANA canónico vigente.
        TZ_ALIASES = {
            'Chile/Continental': 'America/Santiago',
            'America/Buenos_Aires': 'America/Argentina/Buenos_Aires',
        }

        self.base.migrate_table(
            'res_partner',
            is_recursive=True,
            mapping_fields=mapping,
            skip_fields=['message_main_attachment_id', 'category_id'],
            field_transforms={'tz': lambda v: TZ_ALIASES.get(v, v)},
        )

    def _map_sii_activity_description(self):
        """
        Mapea sii_activity_description entre origen y destino por nombre (case-insensitive).
        Los IDs difieren; el nombre es la clave estable.
        En destino el campo name es jsonb {'en_US': '...'}.
        """
        if not self.base.table_exists_in_src('sii_activity_description') or \
                not self.base.table_exists_in_tgt('sii_activity_description'):
            return

        src_rows = self.base.fetch_src("SELECT id, name FROM sii_activity_description")
        self.base.id_map.setdefault('sii_activity_description', {})

        with self.tgt_conn.cursor() as cur:
            cur.execute("SELECT id, name FROM sii_activity_description")
            tgt_map = {}
            for tid, name in cur.fetchall():
                # name puede ser string o jsonb dict
                if isinstance(name, dict):
                    key = (name.get('en_US') or name.get('es_CL') or
                           next(iter(name.values()), '')).lower().strip()
                else:
                    key = (name or '').lower().strip()
                tgt_map[key] = tid

        matched = 0
        for row in src_rows:
            name = row['name']
            if isinstance(name, dict):
                key = (name.get('en_US') or next(iter(name.values()), '')).lower().strip()
            else:
                key = (name or '').lower().strip()
            tgt_id = tgt_map.get(key)
            if tgt_id:
                self.base.id_map['sii_activity_description'][row['id']] = tgt_id
                matched += 1

        log.info("sii_activity_description: %d/%d registros mapeados.", matched, len(src_rows))

    # ──────────────────────────────────────────────
    # Corrección post-migración: partners de empresa
    # ──────────────────────────────────────────────

    def fix_company_partner_names(self):
        """
        Corrige partners de empresa que tienen name=NULL/vacío después de la migración.

        Causa del error en producción:
            l10n_cl_fe._acortar_str() invoca len(self.company_id.partner_id.name)
            donde `name` es False porque el ORM de Odoo retorna False para campos
            Char con valor NULL en BD → TypeError: object of type 'bool' has no len()

        Este método detecta y corrige dos situaciones:
          1. res_company.partner_id apunta a un partner cuyo name es NULL/vacío.
             → UPDATE res_partner.name = company.name
          2. res_company.partner_id es NULL (empresa sin partner vinculado).
             → Vincula un partner existente por nombre o crea uno mínimo.
        """
        log.info("=== fix_company_partner_names: corrigiendo partners de empresa ===")
        fixed = 0

        with self.tgt_conn.cursor() as cur:

            # ── Caso 1: partner vinculado pero con name NULL / vacío / literal 'false' ──
            cur.execute("""
                SELECT rc.id   AS company_id,
                       rc.name AS company_name,
                       rc.partner_id
                FROM   res_company rc
                JOIN   res_partner rp ON rp.id = rc.partner_id
                WHERE  rp.name IS NULL
                   OR  rp.name = ''
                   OR  LOWER(rp.name) = 'false'
            """)
            rows = cur.fetchall()

            for company_id, company_name, partner_id in rows:
                if not company_name or str(company_name).lower() == 'false':
                    company_name = f'Empresa {company_id}'
                cur.execute(
                    "UPDATE res_partner SET name = %s WHERE id = %s",
                    (company_name, partner_id),
                )
                log.warning(
                    "fix_company_partner_names [caso 1]: "
                    "partner_id=%s -> name='%s' (company_id=%s)",
                    partner_id, company_name, company_id,
                )
                fixed += 1

            # ── Caso 2: empresa sin partner_id ──────────────────────────────────────
            cur.execute(
                "SELECT id, name FROM res_company WHERE partner_id IS NULL"
            )
            orphans = cur.fetchall()

            for company_id, company_name in orphans:
                if not company_name or str(company_name).lower() == 'false':
                    company_name = f'Empresa {company_id}'

                # Intentar reutilizar un partner sin empresa con el mismo nombre
                cur.execute(
                    "SELECT id FROM res_partner "
                    "WHERE name = %s AND company_id IS NULL "
                    "LIMIT 1",
                    (company_name,),
                )
                existing = cur.fetchone()

                if existing:
                    partner_id = existing[0]
                    cur.execute(
                        "UPDATE res_company SET partner_id = %s WHERE id = %s",
                        (partner_id, company_id),
                    )
                    log.warning(
                        "fix_company_partner_names [caso 2a]: "
                        "company_id=%s vinculada a partner_id=%s existente",
                        company_id, partner_id,
                    )
                else:
                    # Crear partner mínimo para la empresa
                    # Nota: company_type es campo computado en Odoo 16, no se almacena
                    cur.execute(
                        """INSERT INTO res_partner
                               (name, is_company, active,
                                create_uid, write_uid)
                           VALUES (%s, TRUE, TRUE, 1, 1)
                           RETURNING id""",
                        (company_name,),
                    )
                    partner_id = cur.fetchone()[0]
                    cur.execute(
                        "UPDATE res_company SET partner_id = %s WHERE id = %s",
                        (partner_id, company_id),
                    )
                    log.warning(
                        "fix_company_partner_names [caso 2b]: "
                        "company_id=%s -> partner_id=%s creado nuevo (name='%s')",
                        company_id, partner_id, company_name,
                    )
                fixed += 1

        self.tgt_conn.commit()
        if fixed:
            log.warning(
                "fix_company_partner_names: %d corrección(es) aplicadas. "
                "Reinicia Odoo para que los caches se invaliden.", fixed
            )
        else:
            log.info("fix_company_partner_names: todos los partners de empresa tienen name. OK.")

    # ──────────────────────────────────────────────
    # Productos
    # ──────────────────────────────────────────────

    def migrate_products(self):
        """Migra product.template y product.product."""
        log.info("=== Migrando productos ===")

        # UoM: mapear por nombre
        self._map_uom()

        # Categorías POS (product_template.pos_categ_id): deben migrarse antes,
        # si no toda fila con pos_categ_id NOT NULL viola la FK y se descarta entera.
        self.base.migrate_table('pos_category', is_recursive=True)

        # Campos selection requeridos en v16 que NO existen en v12: si se dejan
        # NULL, la vista de producto falla al grabar con "Campos no válidos"
        # (service_tracking='Crear en el pedido', priority='Favorito').
        #   service_tracking (sale_project): 'no' = no genera proyecto/tarea.
        #   priority         (product):      '0' = prioridad normal.
        # Solo se fuerzan si la columna existe en destino (service_tracking
        # depende del módulo sale_project).
        tmpl_tgt_cols = self.base.get_tgt_columns('product_template')
        tmpl_defaults = {}
        for col, default in (('service_tracking', 'no'), ('priority', '0')):
            if col in tmpl_tgt_cols:
                tmpl_defaults[col] = default

        self.base.migrate_table(
            'product_template',
            mapping_fields={
                'categ_id': 'product_category',
                'uom_id': 'uom_uom',
                'uom_po_id': 'uom_uom',
                'pos_categ_id': 'pos_category',
                # marca_id (method_minori): remapea la marca en productos recién insertados.
                # Para productos ya existentes en destino actúa backfill_product_marca().
                'marca_id': 'method_minori_marcas',
            },
            extra_defaults=tmpl_defaults,
            skip_fields=['message_main_attachment_id'],
        )

        self.base.migrate_table(
            'product_product',
            mapping_fields={'product_tmpl_id': 'product_template'},
            skip_fields=['message_main_attachment_id'],
        )

        # Impuestos de producto (M2M). Sin estos, los productos migrados no
        # tienen IVA y el POS chileno rechaza la boleta afecta con
        # "Debe haber al menos un producto afecto". prod_id -> product.template.
        # Requiere que migrate_taxes() ya haya corrido (id_map['account_tax']).
        self.base.migrate_m2m(
            'product_taxes_rel', 'prod_id', 'tax_id',
            'product_template', 'account_tax'
        )
        self.base.migrate_m2m(
            'product_supplier_taxes_rel', 'prod_id', 'tax_id',
            'product_template', 'account_tax'
        )

        # Rutas de producto (M2M). stock_route_product.product_id referencia
        # product.template (no product.product) tanto en v12 como en v16.
        self.base.migrate_m2m(
            'stock_route_product', 'product_id', 'route_id',
            'product_template', 'stock_route'
        )

    def _map_uom(self):
        """Mapea uom_uom del origen al destino por nombre (en_US, es_CL y normalizado)."""
        import re as _re
        log.info("Mapeando unidades de medida (uom_uom)...")
        src_uoms = self.base.fetch_src("SELECT id, name FROM uom_uom")
        self.base.id_map.setdefault('uom_uom', {})

        with self.tgt_conn.cursor() as cur:
            cur.execute("SELECT id, name FROM uom_uom")
            tgt_en = {}   # en_US key -> id
            tgt_es = {}   # es_CL key -> id
            for tid, name in cur.fetchall():
                if isinstance(name, dict):
                    en = name.get('en_US', '').lower().strip()
                    es = name.get('es_CL', '').lower().strip()
                else:
                    en = str(name).lower().strip()
                    es = ''
                if en:
                    tgt_en[en] = tid
                if es:
                    tgt_es[es] = tid

        def _try_match(key):
            """Prueba variantes del key contra los mapas de destino."""
            t = tgt_en.get(key) or tgt_es.get(key)
            if t:
                return t
            # Strip trailing (s) / (es) / (ft) etc. y reintentar
            norm = _re.sub(r'\([^)]*\)$', '', key).strip()
            if norm != key:
                t = tgt_en.get(norm) or tgt_es.get(norm)
                if t:
                    return t
                # También prueba con 's' al final
                t = tgt_en.get(norm + 's') or tgt_es.get(norm + 's')
                if t:
                    return t
            return None

        matched = 0
        for row in src_uoms:
            src_name = (row['name'] if not isinstance(row['name'], dict)
                        else row['name'].get('en_US', str(row['name'])))
            key = src_name.lower().strip()
            tgt_id = _try_match(key)
            if tgt_id:
                self.base.id_map['uom_uom'][row['id']] = tgt_id
                matched += 1
            else:
                log.warning("uom_uom '%s' (id=%s) sin match en destino.", src_name, row['id'])

        log.info("uom_uom: %d/%d mapeadas.", matched, len(src_uoms))

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

        # user_id (comprador): los usuarios no se migran; se conserva el mismo
        # uid solo si existe en destino, si no queda NULL (evita FK violation).
        self.base.preload_id_map('res_users')

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
                'user_id': 'res_users',
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

        # Agregar RUT de la empresa al nombre de cada secuencia migrada
        # En Odoo 12 el RUT está en res_partner vinculado a la empresa
        rut_rows = self.base.fetch_src(
            "SELECT rc.id, rp.vat FROM res_company rc "
            "JOIN res_partner rp ON rp.id = rc.partner_id "
            "WHERE rp.vat IS NOT NULL"
        )
        rut_map = {}  # {target_company_id: vat}
        for row in rut_rows:
            tgt_cid = self.base.company_mapping.get(row['id'])
            if tgt_cid and row['vat']:
                rut_map[tgt_cid] = row['vat']

        with self.tgt_conn.cursor() as cur:
            seq_ids = list(self.base.id_map.get('ir_sequence', {}).values())
            if seq_ids and rut_map:
                for tgt_cid, rut in rut_map.items():
                    ph = ', '.join(['%s'] * len(seq_ids))
                    cur.execute(
                        f"UPDATE ir_sequence SET name = name || ' - ' || %s "
                        f"WHERE id IN ({ph}) AND company_id = %s",
                        [rut] + seq_ids + [tgt_cid],
                    )
                    log.info("ir_sequence: RUT '%s' agregado a %d secuencias (company_id=%s).",
                             rut, cur.rowcount, tgt_cid)

            # is_dte no existe en Odoo 12; activarlo en secuencias SII recién migradas
            cur.execute(
                """UPDATE ir_sequence SET is_dte = TRUE
                    WHERE sii_document_class_id IS NOT NULL
                      AND (is_dte IS NULL OR is_dte = FALSE)"""
            )
            log.info("ir_sequence: is_dte=True aplicado a %d secuencias SII.", cur.rowcount)
        self.tgt_conn.commit()

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
        skip = {'id', 'message_main_attachment_id', 'caf_file',
                'sii_document_class', 'status'}

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

                # sii_document_class (origen) → document_class_id (destino)
                if 'sii_document_class' in src_cols and 'document_class_id' in tgt_cols:
                    old_dc = row.get('sii_document_class')
                    if old_dc:
                        rec['document_class_id'] = doc_class_map.get(old_dc, old_dc) \
                            if doc_class_map else old_dc

                # status (origen) → state (destino)
                if 'status' in src_cols and 'state' in tgt_cols:
                    status_map = {
                        'draft': 'draft', 'in_use': 'in_use',
                        'spent': 'spent', 'expired': 'expired',
                    }
                    rec['state'] = status_map.get(row.get('status'), row.get('status'))

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

    def migrate_sii_sequences_and_caf(self):
        """
        (1) Renombra las ir.sequence SII del diario de facturas de clientes
            añadiendo '-{target_company_id}' al nombre original para
            diferenciarlas en un entorno multiempresa.
        (2) Reconstruye el id_map de ir_sequence haciendo match por
            sii_document_class_id entre origen y destino.
        (3) Limpia y re-migra dte.caf (relación 1:N con ir.sequence),
            manejando el cambio de nombre de columna
            sii_document_class → document_class_id y status → state.
        """
        log.info("=== Migrando ir.sequence (SII) + dte.caf ===")

        target_company_id = cfg.DEFAULT_TARGET_COMPANY_ID

        # ── 1. Añadir sufijo al nombre de las SII sequences en destino ────────
        with self.tgt_conn.cursor() as cur:
            cur.execute(
                """
                UPDATE ir_sequence
                   SET name = name || %s
                 WHERE sii_document_class_id IS NOT NULL
                   AND company_id = %s
                   AND name NOT LIKE %s
                """,
                (f'-{target_company_id}', target_company_id, f'%-{target_company_id}'),
            )
            updated = cur.rowcount
        self.tgt_conn.commit()
        log.info("ir_sequence SII: %d nombres actualizados con sufijo -%s.", updated, target_company_id)

        # ── 2. Reconstruir id_map ir_sequence por sii_document_class_id ────────
        src_sii_seqs = self.base.fetch_src(
            "SELECT id, sii_document_class_id FROM ir_sequence "
            "WHERE sii_document_class_id IS NOT NULL ORDER BY id"
        )
        doc_class_map = self.accounting._build_sii_doc_class_map()
        self.base.id_map.setdefault('ir_sequence', {})

        with self.tgt_conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(
                "SELECT id, sii_document_class_id FROM ir_sequence "
                "WHERE sii_document_class_id IS NOT NULL AND company_id = %s",
                (target_company_id,),
            )
            tgt_by_docclass = {row['sii_document_class_id']: row['id'] for row in cur.fetchall()}

        mapped = 0
        for row in src_sii_seqs:
            src_dc = row['sii_document_class_id']
            tgt_dc = doc_class_map.get(src_dc, src_dc)
            tgt_seq_id = tgt_by_docclass.get(tgt_dc)
            if tgt_seq_id:
                self.base.id_map['ir_sequence'][row['id']] = tgt_seq_id
                mapped += 1
            else:
                log.warning(
                    "ir_sequence: sin mapeo para src_id=%s sii_document_class_id %s→%s",
                    row['id'], src_dc, tgt_dc,
                )
        log.info("ir_sequence: %d secuencias SII mapeadas en id_map.", mapped)

        # Activar is_dte en todas las secuencias SII de la empresa destino
        # (en Odoo 12 no existía is_dte; el domain del campo sequence_id usa is_dte=True)
        with self.tgt_conn.cursor() as cur:
            cur.execute(
                """UPDATE ir_sequence SET is_dte = TRUE
                    WHERE sii_document_class_id IS NOT NULL
                      AND company_id = %s
                      AND (is_dte IS NULL OR is_dte = FALSE)""",
                (target_company_id,),
            )
            log.info("ir_sequence: %d registros actualizados con is_dte=True.", cur.rowcount)
        self.tgt_conn.commit()

        # ── 3. Migrar dte.caf ────────────────────────────────────────────────
        src_table = next(
            (t for t in ('dte_caf', 'l10n_cl_dte_caf') if self.base.table_exists_in_src(t)), None
        )
        tgt_table = next(
            (t for t in ('dte_caf', 'l10n_cl_dte_caf') if self.base.table_exists_in_tgt(t)), None
        )
        if not src_table or not tgt_table:
            log.warning("dte.caf: tabla no encontrada en origen o destino, saltando.")
            return

        # Limpiar registros previos de la company en destino
        with self.tgt_conn.cursor() as cur:
            cur.execute(f'DELETE FROM "{tgt_table}" WHERE company_id = %s', (target_company_id,))
            deleted = cur.rowcount
        self.tgt_conn.commit()
        log.info("dte_caf: %d registros previos eliminados (company_id=%s).", deleted, target_company_id)

        src_cols = self.base.get_src_columns(src_table)
        tgt_cols = self.base.get_tgt_columns(tgt_table)

        # Columnas que vienen con nombre diferente o no existen en destino
        skip = {'id', 'message_main_attachment_id', 'caf_file',
                'sii_document_class', 'status'}

        # Filtrar source_company_ids mapeados al target_company_id
        src_company_ids = [sid for sid, tid in self.base.company_mapping.items()
                           if tid == target_company_id]
        ph = ','.join(['%s'] * len(src_company_ids))
        rows = self.base.fetch_src(
            f'SELECT * FROM "{src_table}" WHERE company_id IN ({ph}) ORDER BY id',
            params=src_company_ids,
        )

        self.base.id_map.setdefault(src_table, {})
        if src_table != tgt_table:
            self.base.id_map.setdefault(tgt_table, {})

        status_map = {
            'draft': 'draft', 'in_use': 'in_use',
            'spent': 'spent', 'expired': 'expired',
        }

        inserted = 0
        with self.tgt_conn.cursor() as cur:
            for row in rows:
                old_id = row['id']
                rec = {}

                # Copiar campos comunes
                for col in src_cols:
                    if col in skip or col not in tgt_cols:
                        continue
                    rec[col] = row[col]

                # company_id
                rec['company_id'] = self.base.map_company(row.get('company_id'))

                # sii_document_class (origen) → document_class_id (destino)
                if 'sii_document_class' in src_cols and 'document_class_id' in tgt_cols:
                    old_dc = row.get('sii_document_class')
                    if old_dc:
                        rec['document_class_id'] = doc_class_map.get(old_dc, old_dc)

                # sequence_id: mapear via id_map (relación 1:N)
                if 'sequence_id' in src_cols and 'sequence_id' in tgt_cols:
                    old_seq = row.get('sequence_id')
                    rec['sequence_id'] = self.base.id_map.get('ir_sequence', {}).get(old_seq)

                # status (origen) → state (destino)
                if 'status' in src_cols and 'state' in tgt_cols:
                    rec['state'] = status_map.get(row.get('status'), row.get('status'))

                # filename: hacer único por empresa añadiendo _c{company_id} antes de la extensión
                if rec.get('filename'):
                    stem, ext = rec['filename'].rsplit('.', 1) if '.' in rec['filename'] \
                        else (rec['filename'], '')
                    rec['filename'] = f"{stem}_c{target_company_id}.{ext}" if ext \
                        else f"{stem}_c{target_company_id}"

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
                    log.error("dte_caf old_id=%s: %s", old_id, e)

        log.info("dte_caf: %d registros migrados.", inserted)

    def fix_caf_folios(self):
        """
        Calcula y pobla los campos folio_actual y qty_available en la tabla dte_caf 
        (que en Odoo 16 son requeridos y causan error si están nulos).
        La lógica se basa en el number_next de la secuencia (ir_sequence) asociada.
        """
        log.info("=== Corrigiendo folios vacíos en dte_caf ===")
        tgt_table = next(
            (t for t in ('dte_caf', 'l10n_cl_dte_caf') if self.base.table_exists_in_tgt(t)), None
        )
        if not tgt_table:
            log.warning("Tabla dte_caf no encontrada en destino.")
            return

        with self.tgt_conn.cursor() as cur:
            # FIX DOCUMENT_CLASS_ID
            # En Odoo 12, sii_document_class era el código real del SII (ej. 33, 61).
            # En Odoo 16, document_class_id es un FK a sii_document_class (ID interno).
            # Esto causa que los CAF queden asignados a clases equivocadas (ej ID 33 = Registro Mercadería).
            # 1. Corregimos mediante la secuencia asociada (si existe).
            cur.execute(f"""
                UPDATE {tgt_table} c
                SET document_class_id = jc.sii_document_class_id
                FROM account_journal_sii_document_class jc
                WHERE c.sequence_id = jc.sequence_id
                  AND c.document_class_id != jc.sii_document_class_id
            """)
            docs_updated1 = cur.rowcount
            # 2. Corregimos el resto asumiendo que el ID migrado es en realidad el sii_code
            cur.execute(f"""
                UPDATE {tgt_table} c
                SET document_class_id = dc.id
                FROM sii_document_class dc
                WHERE c.document_class_id IN (33, 34, 39, 41, 46, 52, 56, 61, 110, 111, 112)
                  AND dc.sii_code = c.document_class_id
            """)
            docs_updated2 = cur.rowcount
            if docs_updated1 > 0 or docs_updated2 > 0:
                log.info("Corregido document_class_id en %d registros de dte_caf (usando secuencia) y %d (fallback)", docs_updated1, docs_updated2)

            # FIX COMPANY_ID
            # Odoo 16 exige company_id explícito en dte_caf para hacer el match en _timbrar()
            # Actualizamos company_id a partir de la secuencia (ir_sequence)
            cur.execute(f"""
                UPDATE {tgt_table} c
                SET company_id = sq.company_id
                FROM ir_sequence sq
                WHERE sq.id = c.sequence_id
                  AND c.company_id IS NULL
                  AND sq.company_id IS NOT NULL
            """)
            company_updated = cur.rowcount
            if company_updated > 0:
                log.info("Corregido company_id nulo en %d registros de dte_caf (usando secuencia)", company_updated)

            # Fallback a empresa principal (ID 1) si aún quedan en NULL, para evitar error de visibilidad
            cur.execute(f"""
                UPDATE {tgt_table}
                SET company_id = 1
                WHERE company_id IS NULL
            """)
            fallback_updated = cur.rowcount
            if fallback_updated > 0:
                log.info("Corregido company_id nulo en %d registros de dte_caf (fallback a 1)", fallback_updated)
            
            cur.execute(f"""
                UPDATE {tgt_table} c
                SET folio_actual = 
                    CASE 
                        WHEN s.number_next > c.final_nm THEN c.final_nm + 1
                        WHEN s.number_next < c.start_nm THEN c.start_nm
                        ELSE s.number_next
                    END,
                    qty_available = 
                    CASE 
                        WHEN s.number_next > c.final_nm THEN 0
                        WHEN s.number_next < c.start_nm THEN c.final_nm - c.start_nm + 1
                        ELSE c.final_nm - s.number_next + 1
                    END
                FROM ir_sequence s
                WHERE c.sequence_id = s.id
                  AND c.folio_actual IS NULL
            """)
            updated_with_seq = cur.rowcount

            # Para los que no tienen secuencia o s.number_next es nulo, asumimos que no se han usado
            cur.execute(f"""
                UPDATE {tgt_table}
                SET folio_actual = start_nm,
                    qty_available = final_nm - start_nm + 1
                WHERE folio_actual IS NULL
            """)
            updated_no_seq = cur.rowcount

            # Actualizar estado a 'spent' si qty_available es <= 0 y está in_use
            cur.execute(f"""
                UPDATE {tgt_table}
                SET state = 'spent'
                WHERE qty_available <= 0 AND state = 'in_use'
            """)
            spent_updated = cur.rowcount

            # WORKAROUND PARA BUG EN L10N_CL_FE DE ODOO 16:
            # Los CAF en estado 'draft' tienen cantidad_folios = 0 por código Python,
            # lo que provoca ZeroDivisionError en _used_level al cargar la vista lista.
            # Pasamos los 'draft' a 'in_use' para evitar que se caiga la vista.
            cur.execute(f"""
                UPDATE {tgt_table}
                SET state = 'in_use'
                WHERE state = 'draft'
            """)
            draft_updated = cur.rowcount

        self.tgt_conn.commit()
        log.info("dte_caf folios corregidos: %d (con sec) + %d (sin sec). Pasados a 'spent': %d, 'draft'->'in_use': %d", 
                 updated_with_seq, updated_no_seq, spent_updated, draft_updated)

    def fix_caf_files(self):
        """
        En Odoo 16 el XML del CAF se lee desde el campo binario `caf_file`, el cual se almacena 
        en la tabla `ir_attachment`. 
        Este método toma el `caf_string` (XML en texto) copiado en la tabla dte_caf, 
        lo codifica en base64 y genera el registro en ir_attachment para que Odoo lo reconozca.
        """
        log.info("=== Generando attachments para caf_file desde caf_string ===")
        import base64
        tgt_table = next(
            (t for t in ('dte_caf', 'l10n_cl_dte_caf') if self.base.table_exists_in_tgt(t)), None
        )
        if not tgt_table:
            log.warning("Tabla dte_caf no encontrada en destino.")
            return

        with self.tgt_conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(f"""
                SELECT c.id, c.filename, c.caf_string, c.company_id 
                FROM {tgt_table} c
                LEFT JOIN ir_attachment a ON a.res_model = 'dte.caf' 
                                         AND a.res_field = 'caf_file' 
                                         AND a.res_id = c.id
                WHERE c.caf_string IS NOT NULL 
                  AND a.id IS NULL
            """)
            rows = cur.fetchall()

        if not rows:
            log.info("No hay CAFs pendientes de generar attachment.")
            return

        inserted = 0
        with self.tgt_conn.cursor() as cur:
            for row in rows:
                xml_string = row['caf_string']
                # Odoo 16 almacena los bytes en crudo en db_datas, el ORM es quien codifica a b64 al leer.
                raw_data = xml_string.encode('utf-8')
                
                filename = row['filename'] or f"caf_{row['id']}.xml"
                
                cur.execute("""
                    INSERT INTO ir_attachment (
                        name, res_model, res_field, res_id, company_id, 
                        type, db_datas, mimetype, create_date, write_date, create_uid, write_uid
                    ) VALUES (
                        %s, 'dte.caf', 'caf_file', %s, %s,
                        'binary', %s, 'text/xml', NOW(), NOW(), 1, 1
                    )
                """, (filename, row['id'], row['company_id'], raw_data))
                inserted += 1

        self.tgt_conn.commit()
        log.info("Attachments de caf_file creados: %d", inserted)


    def migrate_journal_sii_sequences_caf(self, journal_type: str = 'sale'):
        """
        Migra ir_sequence + dte_caf SOLO para el diario de Facturas de Clientes
        (type='sale'), usando account_journal_sii_document_class como tabla puente.

        Flujo:
          1. Localizar el journal de venta en origen.
          2. Obtener sus entradas en account_journal_sii_document_class.
          3. Migrar las ir_sequence de esas entradas (si no están ya en id_map).
          4. Migrar los dte_caf asociados a esas sequences.
          5. Migrar account_journal_sii_document_class vinculando al journal destino.
        """
        log.info("=== Migrando SII: ir_sequence + dte_caf para Facturas de Clientes ===")

        SII_JDC_TABLE = 'account_journal_sii_document_class'

        # ── 1. Journal de venta en origen ────────────────────────────────────
        sale_journals = self.base.fetch_src(
            "SELECT id, name, code FROM account_journal WHERE type=%s ORDER BY id",
            (journal_type,)
        )
        if not sale_journals:
            log.warning("No se encontraron journals type='%s' en origen.", journal_type)
            return

        src_journal = sale_journals[0]
        src_journal_id = src_journal['id']
        log.info("Journal origen: id=%s  code=%s  name=%s",
                 src_journal_id, src_journal['code'], src_journal['name'])

        # ── 2. Entradas en account_journal_sii_document_class ────────────────
        if not self.base.table_exists_in_src(SII_JDC_TABLE):
            log.warning("Tabla %s no encontrada en origen, saltando.", SII_JDC_TABLE)
            return

        jdc_rows = self.base.fetch_src(
            f"SELECT * FROM {SII_JDC_TABLE} WHERE journal_id=%s ORDER BY sequence, id",
            (src_journal_id,)
        )
        if not jdc_rows:
            log.warning("No hay entradas en %s para journal_id=%s.", SII_JDC_TABLE, src_journal_id)
            return

        seq_ids = [r['sequence_id'] for r in jdc_rows if r.get('sequence_id')]
        log.info("  %d document_classes encontradas, sequences: %s", len(jdc_rows), seq_ids)

        # ── 3. Migrar ir_sequence filtradas ───────────────────────────────────
        if seq_ids:
            self.base.id_map.setdefault('ir_sequence', {})
            tgt_seq_cols = self.base.get_tgt_columns('ir_sequence')
            src_seq_cols = self.base.get_src_columns('ir_sequence')
            skip_seq = {'id', 'message_main_attachment_id'}
            skip_seq |= {c for c in src_seq_cols if c not in tgt_seq_cols}

            ph = ', '.join(['%s'] * len(seq_ids))
            seq_rows = self.base.fetch_src(
                f"SELECT * FROM ir_sequence WHERE id IN ({ph}) ORDER BY id",
                seq_ids
            )

            with self.tgt_conn.cursor() as cur:
                for seq in seq_rows:
                    old_id = seq['id']
                    if old_id in self.base.id_map['ir_sequence']:
                        log.debug("ir_sequence old_id=%s ya en id_map, saltando.", old_id)
                        continue

                    rec = {}
                    for col in src_seq_cols:
                        if col in skip_seq or col not in tgt_seq_cols:
                            continue
                        rec[col] = seq[col]

                    rec['company_id'] = self.base.map_company(seq.get('company_id'))
                    rec['create_uid'] = 1
                    rec['write_uid'] = 1

                    for f in list(rec.keys()):
                        if f.endswith('_id') and rec[f] == 0:
                            rec[f] = None

                    self.base._fill_not_null(rec, tgt_seq_cols)

                    cols_q = ', '.join(f'"{c}"' for c in rec)
                    placeholders = ', '.join(['%s'] * len(rec))
                    try:
                        cur.execute(
                            f'INSERT INTO ir_sequence ({cols_q}) VALUES ({placeholders}) RETURNING id',
                            self.base.prepare_vals(rec, tgt_seq_cols),
                        )
                        new_id = cur.fetchone()[0]
                        self.base.id_map['ir_sequence'][old_id] = new_id
                        log.info("  ir_sequence old_id=%s -> new_id=%s  (%s)",
                                 old_id, new_id, seq.get('name', ''))
                    except Exception as e:
                        self.tgt_conn.rollback()
                        log.error("ir_sequence old_id=%s: %s", old_id, e)

            # Sub-rangos de secuencia
            if self.base.table_exists_in_src('ir_sequence_date_range') and \
                    self.base.table_exists_in_tgt('ir_sequence_date_range'):
                ph = ', '.join(['%s'] * len(seq_ids))
                dr_rows = self.base.fetch_src(
                    f"SELECT * FROM ir_sequence_date_range WHERE sequence_id IN ({ph}) ORDER BY id",
                    seq_ids
                )
                self.base.id_map.setdefault('ir_sequence_date_range', {})
                tgt_dr_cols = self.base.get_tgt_columns('ir_sequence_date_range')
                dr_inserted = 0
                with self.tgt_conn.cursor() as cur:
                    for dr in dr_rows:
                        new_seq_id = self.base.id_map['ir_sequence'].get(dr['sequence_id'])
                        if not new_seq_id:
                            continue
                        rec = {
                            'sequence_id': new_seq_id,
                            'date_from': dr.get('date_from'),
                            'date_to': dr.get('date_to'),
                            'number_next_actual': dr.get('number_next_actual', 1),
                            'create_uid': 1,
                            'write_uid': 1,
                        }
                        self.base._fill_not_null(rec, tgt_dr_cols)
                        cols_q = ', '.join(f'"{c}"' for c in rec)
                        placeholders = ', '.join(['%s'] * len(rec))
                        try:
                            cur.execute(
                                f'INSERT INTO ir_sequence_date_range ({cols_q}) VALUES ({placeholders})',
                                self.base.prepare_vals(rec, tgt_dr_cols),
                            )
                            dr_inserted += 1
                        except Exception as e:
                            self.tgt_conn.rollback()
                            log.warning("ir_sequence_date_range: %s", e)
                log.info("  ir_sequence_date_range: %d rangos migrados.", dr_inserted)

        # ── 4. Migrar dte_caf para esas sequences ─────────────────────────────
        caf_src = next((t for t in ('dte_caf', 'l10n_cl_dte_caf')
                        if self.base.table_exists_in_src(t)), None)
        caf_tgt = next((t for t in ('dte_caf', 'l10n_cl_dte_caf')
                        if self.base.table_exists_in_tgt(t)), None)

        if caf_src and caf_tgt and seq_ids:
            ph = ', '.join(['%s'] * len(seq_ids))
            caf_rows = self.base.fetch_src(
                f"SELECT * FROM {caf_src} WHERE sequence_id IN ({ph}) ORDER BY id",
                seq_ids
            )
            src_caf_cols = self.base.get_src_columns(caf_src)
            tgt_caf_cols = self.base.get_tgt_columns(caf_tgt)
            skip_caf = {'id', 'message_main_attachment_id', 'sii_document_class', 'status', 'caf_file'}
            doc_class_map = self.accounting._build_sii_doc_class_map()
            status_map = {'draft': 'draft', 'in_use': 'in_use',
                          'spent': 'spent', 'expired': 'expired'}

            self.base.id_map.setdefault(caf_src, {})
            if caf_src != caf_tgt:
                self.base.id_map.setdefault(caf_tgt, {})

            caf_inserted = 0
            with self.tgt_conn.cursor() as cur:
                for row in caf_rows:
                    old_id = row['id']
                    # Ya migrado por migrate_dte_caf() — solo actualizar id_map si falta
                    if old_id in self.base.id_map.get(caf_src, {}):
                        caf_inserted += 1
                        continue
                    rec = {}
                    for col in src_caf_cols:
                        if col in skip_caf or col not in tgt_caf_cols:
                            continue
                        rec[col] = row[col]

                    rec['company_id'] = self.base.map_company(row.get('company_id'))

                    if 'sequence_id' in src_caf_cols and 'sequence_id' in tgt_caf_cols:
                        rec['sequence_id'] = self.base.id_map['ir_sequence'].get(row.get('sequence_id'))

                    if 'journal_id' in src_caf_cols and 'journal_id' in tgt_caf_cols:
                        rec['journal_id'] = self.base.id_map.get('account_journal', {}).get(
                            row.get('journal_id'))

                    if 'sii_document_class' in src_caf_cols and 'document_class_id' in tgt_caf_cols:
                        old_dc = row.get('sii_document_class')
                        if old_dc:
                            rec['document_class_id'] = doc_class_map.get(old_dc, old_dc)

                    if 'status' in src_caf_cols and 'state' in tgt_caf_cols:
                        rec['state'] = status_map.get(row.get('status'), row.get('status') or 'draft')

                    for f in list(rec.keys()):
                        if f.endswith('_id') and rec[f] == 0:
                            rec[f] = None

                    rec['create_uid'] = 1
                    rec['write_uid'] = 1
                    self.base._fill_not_null(rec, tgt_caf_cols)

                    cols_q = ', '.join(f'"{c}"' for c in rec)
                    placeholders = ', '.join(['%s'] * len(rec))
                    try:
                        cur.execute(
                            f'INSERT INTO "{caf_tgt}" ({cols_q}) VALUES ({placeholders}) RETURNING id',
                            self.base.prepare_vals(rec, tgt_caf_cols),
                        )
                        new_id = cur.fetchone()[0]
                        self.base.id_map[caf_src][old_id] = new_id
                        if caf_src != caf_tgt:
                            self.base.id_map[caf_tgt][old_id] = new_id
                        caf_inserted += 1
                    except Exception as e:
                        self.tgt_conn.rollback()
                        log.error("dte_caf old_id=%s: %s", old_id, e)

            log.info("  dte_caf: %d registros migrados.", caf_inserted)
        else:
            log.info("  dte_caf: tabla no encontrada o sin sequences, saltando.")

        # ── 5. Migrar account_journal_sii_document_class ─────────────────────
        if not self.base.table_exists_in_tgt(SII_JDC_TABLE):
            log.warning("Tabla %s no existe en destino, saltando.", SII_JDC_TABLE)
            return

        # Mapear journal de venta en destino
        tgt_journal_id = self.base.id_map.get('account_journal', {}).get(src_journal_id)
        if not tgt_journal_id:
            # Buscar en destino por code
            with self.tgt_conn.cursor() as cur:
                cur.execute(
                    "SELECT id FROM account_journal WHERE code=%s AND company_id=%s LIMIT 1",
                    (src_journal['code'], self.base.map_company(None))
                )
                row = cur.fetchone()
                tgt_journal_id = row[0] if row else None

        if not tgt_journal_id:
            log.warning("No se encontró journal destino para src_id=%s, "
                        "saltando account_journal_sii_document_class.", src_journal_id)
            return

        tgt_jdc_cols = self.base.get_tgt_columns(SII_JDC_TABLE)
        doc_class_map = self.accounting._build_sii_doc_class_map()

        jdc_inserted = 0
        with self.tgt_conn.cursor() as cur:
            for row in jdc_rows:
                old_id = row['id']

                # document_class: en destino puede ser sii_document_class_id mapeado
                new_dc_id = doc_class_map.get(row.get('sii_document_class_id'),
                                              row.get('sii_document_class_id'))

                new_seq_id = self.base.id_map.get('ir_sequence', {}).get(
                    row.get('sequence_id'))

                rec = {
                    'journal_id': tgt_journal_id,
                    'sii_document_class_id': new_dc_id,
                    'sequence_id': new_seq_id,
                    'sequence': row.get('sequence', 0),
                    'company_id': self.base.map_company(row.get('company_id')),
                    'create_uid': 1,
                    'write_uid': 1,
                }

                # Limpiar FK=0
                for f in list(rec.keys()):
                    if f.endswith('_id') and rec[f] == 0:
                        rec[f] = None

                # Filtrar campos que no existen en destino
                rec = {k: v for k, v in rec.items() if k in tgt_jdc_cols}
                self.base._fill_not_null(rec, tgt_jdc_cols)

                cols_q = ', '.join(f'"{c}"' for c in rec)
                placeholders = ', '.join(['%s'] * len(rec))
                try:
                    cur.execute(
                        f'INSERT INTO {SII_JDC_TABLE} ({cols_q}) VALUES ({placeholders}) '
                        f'ON CONFLICT DO NOTHING',
                        self.base.prepare_vals(rec, tgt_jdc_cols),
                    )
                    jdc_inserted += 1
                except Exception as e:
                    self.tgt_conn.rollback()
                    log.error("account_journal_sii_document_class old_id=%s: %s", old_id, e)

        log.info("  account_journal_sii_document_class: %d entradas migradas "
                 "(journal_id destino=%s).", jdc_inserted, tgt_journal_id)

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

                # sii_firma en destino usa company_ids (many2many), no company_id
                # La relación se inserta en res_company_sii_firma_rel después del INSERT

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
                    # Vincular al company_ids mediante la tabla many2many
                    target_company_id = self.base.map_company(row.get('company_id'))
                    cur.execute(
                        'INSERT INTO res_company_sii_firma_rel (sii_firma_id, res_company_id) '
                        'VALUES (%s, %s) ON CONFLICT DO NOTHING',
                        (new_id, target_company_id),
                    )
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

            # 0a. Normalizar company_id de datos preexistentes
            self.normalize_existing_company_id()

            # 0b. Empresas
            self.setup_companies()

            # 1. Limpieza
            self.clean_target_data()

            # 2. Configuración base (monedas, condiciones de pago, categorías)
            self.migrate_base_config()

            # 3. Contactos
            self.migrate_partners()
            # 3b. Corrección: asegurar que todos los partners de empresa tengan name
            #     (evita TypeError en l10n_cl_fe._acortar_str al validar facturas)
            self.fix_company_partner_names()

            # 3c. Usuarios (requiere id_map de res_partner ya cargado)
            self.users.migrate_users()

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

            # 9b. Modelos de method_minori (marcas, periodos, backfill marca_id)
            self.method_minori.migrate_all()

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

            # 12c. Reparaciones
            self.repair.migrate_all()
            self.stock.update_moves_repair_id()  # 2do paso: vincula stock_move.repair_id

            # 12b. CAF, firma y secuencias SII localización chilena
            self.migrate_sii_firma()
            self.migrate_dte_caf()
            # Secuencias + CAF + account_journal_sii_document_class para Facturas de Clientes
            self.migrate_journal_sii_sequences_caf()
            # Correcciones post-migración de CAF
            self.fix_caf_folios()
            self.fix_caf_files()

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
