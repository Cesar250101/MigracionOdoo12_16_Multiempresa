"""
base_migrator.py - Clase base con utilidades comunes de migración
Conexión directa psycopg2, sin XML-RPC.
"""

import logging
import psycopg2
import psycopg2.extras
from psycopg2.extras import Json

log = logging.getLogger(__name__)


class BaseMigrator:
    """
    Clase base para migración directa DB-to-DB (Odoo 12 -> Odoo 16).
    Provee métodos genéricos para migrar tablas, M2M y actualizar secuencias.
    """

    def __init__(self, src_conn: psycopg2.extensions.connection,
                 tgt_conn: psycopg2.extensions.connection,
                 company_mapping: dict):
        """
        Args:
            src_conn: Conexión a la BD origen (Odoo 12)
            tgt_conn: Conexión a la BD destino (Odoo 16)
            company_mapping: {old_company_id: new_company_id}
        """
        self.src_conn = src_conn
        self.tgt_conn = tgt_conn
        self.company_mapping = company_mapping
        # id_map[table] = {old_id: new_id}
        self.id_map: dict[str, dict] = {}
        # Tipo de cuenta por account_id: {account_id: 'asset_receivable'|...}
        self.account_type_cache: dict[int, str] = {}

    # ─────────────────────────────────────────────────────────────
    # Utilidades de esquema
    # ─────────────────────────────────────────────────────────────

    def get_src_columns(self, table: str) -> set:
        """Retorna el conjunto de columnas que existen en origen."""
        with self.src_conn.cursor() as cur:
            cur.execute(
                "SELECT column_name FROM information_schema.columns "
                "WHERE table_name=%s AND table_schema='public'", (table,)
            )
            return {r[0] for r in cur.fetchall()}

    def get_tgt_columns(self, table: str) -> dict:
        """
        Retorna {column_name: {'nullable': bool, 'type': str}}
        para la tabla en destino.
        """
        with self.tgt_conn.cursor() as cur:
            cur.execute(
                "SELECT column_name, is_nullable, data_type "
                "FROM information_schema.columns "
                "WHERE table_name=%s AND table_schema='public'", (table,)
            )
            return {
                r[0]: {'nullable': r[1] == 'YES', 'type': r[2]}
                for r in cur.fetchall()
            }

    def table_exists_in_src(self, table: str) -> bool:
        with self.src_conn.cursor() as cur:
            cur.execute(
                "SELECT EXISTS(SELECT 1 FROM information_schema.tables "
                "WHERE table_name=%s AND table_schema='public')", (table,)
            )
            return cur.fetchone()[0]

    def table_exists_in_tgt(self, table: str) -> bool:
        with self.tgt_conn.cursor() as cur:
            cur.execute(
                "SELECT EXISTS(SELECT 1 FROM information_schema.tables "
                "WHERE table_name=%s AND table_schema='public')", (table,)
            )
            return cur.fetchone()[0]

    # ─────────────────────────────────────────────────────────────
    # Mapeo de company_id
    # ─────────────────────────────────────────────────────────────

    def map_company(self, old_id) -> int:
        """Retorna el nuevo company_id para un old_id. Usa el primer destino como fallback."""
        if old_id is None:
            return next(iter(self.company_mapping.values()))
        return self.company_mapping.get(old_id, next(iter(self.company_mapping.values())))

    # ─────────────────────────────────────────────────────────────
    # Migración genérica de tabla
    # ─────────────────────────────────────────────────────────────

    def migrate_table(
        self,
        table: str,
        mapping_fields: dict = None,
        filter_sql: str = "",
        skip_fields: list = None,
        is_recursive: bool = False,
        recursive_field: str = 'parent_id',
        extra_defaults: dict = None,
        field_transforms: dict = None,
    ):
        """
        Migra una tabla completa de origen a destino.

        Args:
            table: nombre de la tabla PostgreSQL
            mapping_fields: {campo: tabla_referenciada} para traducir FKs
            filter_sql: cláusula WHERE adicional (sin la palabra WHERE)
            skip_fields: campos a omitir en el SELECT origen
            is_recursive: si la tabla tiene parent_id auto-referencial
            recursive_field: nombre del campo recursivo (default: parent_id)
            extra_defaults: {campo: valor} a forzar en destino
            field_transforms: {campo: callable(valor) -> valor}
        """
        if not self.table_exists_in_src(table):
            log.warning("Tabla %s no existe en origen, saltando.", table)
            return
        if not self.table_exists_in_tgt(table):
            log.warning("Tabla %s no existe en destino, saltando.", table)
            return

        tgt_cols = self.get_tgt_columns(table)
        src_cols = self.get_src_columns(table)
        skip_fields = set(skip_fields or [])

        cols_to_fetch = [
            c for c in tgt_cols
            if c != 'id' and c in src_cols and c not in skip_fields
        ]

        order_clause = ""
        if is_recursive:
            order_clause = f"ORDER BY {recursive_field} NULLS FIRST, id"
        elif table == 'res_partner':
            order_clause = (
                "ORDER BY CASE WHEN commercial_partner_id=id THEN 0 ELSE 1 END, "
                "commercial_partner_id NULLS FIRST, id"
            )

        where = f"WHERE {filter_sql}" if filter_sql else ""
        select_cols = ", ".join(['id'] + [f'"{c}"' for c in cols_to_fetch])
        query = f'SELECT {select_cols} FROM "{table}" {where} {order_clause}'

        with self.src_conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as src_cur:
            src_cur.execute(query)
            rows = src_cur.fetchall()

        if table not in self.id_map:
            self.id_map[table] = {}

        inserted = 0
        skipped = 0

        with self.tgt_conn.cursor() as tgt_cur:
            for row in rows:
                row_dict = dict(row)
                old_id = row_dict.pop('id')

                # Mapear company_id
                if 'company_id' in tgt_cols:
                    old_cid = row_dict.get('company_id')
                    row_dict['company_id'] = self.map_company(old_cid)

                # Aplicar transformaciones de campos
                if field_transforms:
                    for field, transform in field_transforms.items():
                        if field in row_dict and transform is not None:
                            row_dict[field] = transform(row_dict[field])

                # Mapear FKs
                if mapping_fields:
                    for field, ref_table in mapping_fields.items():
                        val = row_dict.get(field)
                        if val:
                            new_ref = self.id_map.get(ref_table, {}).get(val)
                            row_dict[field] = new_ref  # None si no está mapeado

                # Recursividad
                if is_recursive and recursive_field in row_dict:
                    old_p = row_dict[recursive_field]
                    row_dict[recursive_field] = self.id_map[table].get(old_p) if old_p else None

                # res_partner: commercial_partner_id
                if table == 'res_partner':
                    old_comm = row_dict.get('commercial_partner_id')
                    if old_comm == old_id:
                        row_dict['commercial_partner_id'] = None  # Se actualiza post-insert
                    elif old_comm:
                        row_dict['commercial_partner_id'] = self.id_map[table].get(old_comm)

                # Defaults del sistema
                if 'create_uid' in row_dict:
                    row_dict['create_uid'] = 1
                if 'write_uid' in row_dict:
                    row_dict['write_uid'] = 1

                # Extra defaults (forzados)
                if extra_defaults:
                    row_dict.update(extra_defaults)

                # Convertir FK=0 a None
                for f in list(row_dict.keys()):
                    if f.endswith('_id') and row_dict[f] == 0:
                        row_dict[f] = None

                # Rellenar NOT NULL sin valor
                self._fill_not_null(row_dict, tgt_cols)

                # Preparar valores: coerce strings a jsonb cuando el destino lo requiere
                cols = list(row_dict.keys())
                vals = [self._coerce_value(c, v, tgt_cols) for c, v in zip(cols, row_dict.values())]
                cols_q = ', '.join(f'"{c}"' for c in cols)
                placeholders = ', '.join(['%s'] * len(cols))
                sql = f'INSERT INTO "{table}" ({cols_q}) VALUES ({placeholders}) RETURNING id'

                try:
                    tgt_cur.execute(sql, vals)
                    new_id = tgt_cur.fetchone()[0]
                    self.id_map[table][old_id] = new_id
                    inserted += 1

                    # Actualizar commercial_partner_id a sí mismo
                    if table == 'res_partner':
                        old_comm = dict(row).get('commercial_partner_id')
                        if old_comm == old_id:
                            tgt_cur.execute(
                                "UPDATE res_partner SET commercial_partner_id=%s WHERE id=%s",
                                (new_id, new_id)
                            )
                except psycopg2.errors.UniqueViolation:
                    self.tgt_conn.rollback()
                    existing = self._find_existing(table, row_dict, tgt_cur)
                    if existing:
                        self.id_map[table][old_id] = existing
                        skipped += 1
                    else:
                        log.warning("Duplicado sin match en %s old_id=%s", table, old_id)
                except psycopg2.errors.NotNullViolation as e:
                    # FK no mapeada o campo requerido sin valor → saltar registro con aviso
                    self.tgt_conn.rollback()
                    log.warning("Saltando %s old_id=%s (NOT NULL sin valor): %s",
                                table, old_id, str(e).split('\n')[0])
                except psycopg2.errors.ForeignKeyViolation as e:
                    # FK apunta a ID inexistente en destino → saltar registro con aviso
                    self.tgt_conn.rollback()
                    log.warning("Saltando %s old_id=%s (FK violation): %s",
                                table, old_id, str(e).split('\n')[0])
                except Exception as e:
                    self.tgt_conn.rollback()
                    log.error("Error en %s old_id=%s: %s", table, old_id, e)
                    raise

        log.info("%-35s inseridos=%-6d saltados=%d", table, inserted, skipped)

    # ─────────────────────────────────────────────────────────────
    # Migración M2M
    # ─────────────────────────────────────────────────────────────

    def migrate_m2m(self, table: str, field1: str, field2: str,
                    ref_table1: str, ref_table2: str):
        """Migra tabla relacional M2M mapeando IDs."""
        if not self.table_exists_in_src(table):
            return

        with self.src_conn.cursor() as cur:
            try:
                cur.execute(f'SELECT "{field1}", "{field2}" FROM "{table}"')
                rows = cur.fetchall()
            except Exception:
                self.src_conn.rollback()
                return

        inserted = 0
        with self.tgt_conn.cursor() as tgt_cur:
            for f1_old, f2_old in rows:
                f1_new = self.id_map.get(ref_table1, {}).get(f1_old)
                f2_new = self.id_map.get(ref_table2, {}).get(f2_old)
                if not f1_new or not f2_new:
                    continue
                try:
                    tgt_cur.execute(
                        f'INSERT INTO "{table}" ("{field1}", "{field2}") VALUES (%s, %s)',
                        (f1_new, f2_new)
                    )
                    inserted += 1
                except Exception:
                    self.tgt_conn.rollback()

        log.info("M2M %-35s inseridos=%d", table, inserted)

    # ─────────────────────────────────────────────────────────────
    # Secuencias
    # ─────────────────────────────────────────────────────────────

    def update_sequences(self):
        """Actualiza todas las secuencias PostgreSQL al máximo de sus tablas."""
        log.info("Actualizando secuencias PostgreSQL...")
        with self.tgt_conn.cursor() as cur:
            cur.execute("""
                SELECT
                    'SELECT SETVAL(' ||
                    quote_literal(quote_ident(t.relname) || '_' || quote_ident(a.attname) || '_seq') ||
                    ', COALESCE(MAX(' || quote_ident(a.attname) || '), 1)) FROM ' ||
                    quote_ident(t.relname) || ';'
                FROM pg_class t
                JOIN pg_attribute a ON a.attrelid = t.oid
                JOIN pg_namespace n ON n.oid = t.relnamespace
                WHERE a.attnum > 0
                  AND NOT a.attisdropped
                  AND n.nspname = 'public'
                  AND t.relkind = 'r'
                  AND pg_get_serial_sequence(
                        quote_ident(t.relname),
                        a.attname
                      ) IS NOT NULL
            """)
            queries = [r[0] for r in cur.fetchall()]
            for q in queries:
                try:
                    cur.execute(q)
                except Exception:
                    continue
        log.info("Secuencias actualizadas.")

    def fix_ir_sequences(self):
        """Crea las secuencias PostgreSQL faltantes para ir.sequence."""
        log.info("Reparando secuencias ir.sequence...")
        fixed = 0
        with self.tgt_conn.cursor() as cur:
            cur.execute("""
                SELECT id, name, implementation, number_next, number_increment
                FROM ir_sequence
                WHERE implementation IN ('standard', 'no_gap')
                ORDER BY id
            """)
            for seq_id, name, impl, number_next, number_increment in cur.fetchall():
                pg_name = f'ir_sequence_{seq_id:03d}'
                cur.execute(
                    "SELECT EXISTS(SELECT 1 FROM pg_class WHERE relkind='S' AND relname=%s)",
                    (pg_name,)
                )
                if not cur.fetchone()[0]:
                    nxt = number_next or 1
                    inc = number_increment or 1
                    try:
                        cur.execute(
                            f"CREATE SEQUENCE {pg_name} "
                            f"INCREMENT BY {inc} START WITH {nxt} "
                            f"NO MINVALUE NO MAXVALUE CACHE 1"
                        )
                        if nxt > 1:
                            cur.execute(f"SELECT setval('{pg_name}', {nxt}, false)")
                        fixed += 1
                    except Exception as e:
                        log.warning("No se pudo crear secuencia %s: %s", pg_name, e)
        log.info("Secuencias ir.sequence reparadas: %d", fixed)

    # ─────────────────────────────────────────────────────────────
    # Helpers internos
    # ─────────────────────────────────────────────────────────────

    def _coerce_value(self, col: str, val, tgt_cols: dict):
        """
        Convierte el valor al tipo correcto para la columna de destino.
        Caso crítico Odoo 12->16: campos traducibles son jsonb en v16 pero
        varchar en v12. Un string 'Contado' debe quedar como {"en_US": "Contado"}.
        """
        if val is None:
            return None
        dtype = tgt_cols.get(col, {}).get('type', '')
        if 'json' in dtype:
            if isinstance(val, dict):
                return Json(val)
            # String plano -> envolver en jsonb translatable
            if isinstance(val, str):
                return Json({'en_US': val})
            # Número u otro tipo -> convertir a string dentro de json
            return Json({'en_US': str(val)})
        if isinstance(val, dict):
            # dict en columna no-json: serializar como texto (fallback seguro)
            return str(val)
        return val

    def _fill_not_null(self, row_dict: dict, tgt_cols: dict):
        """Rellena campos NOT NULL sin valor con defaults seguros."""
        for col, info in tgt_cols.items():
            if col == 'id' or info['nullable']:
                continue
            val = row_dict.get(col)
            if val is not None:
                continue
            dtype = info['type']
            if col.endswith('_id'):
                # No inyectamos FK forzadas aquí - se deja NULL y se maneja por el caller
                pass
            elif 'int' in dtype or 'numeric' in dtype or 'double' in dtype or 'real' in dtype:
                row_dict[col] = 0
            elif 'bool' in dtype:
                row_dict[col] = False
            elif 'json' in dtype:
                row_dict[col] = Json({'en_US': ''})
            elif 'char' in dtype or 'text' in dtype:
                row_dict[col] = ''

    def _find_existing(self, table: str, row_dict: dict, cur) -> int:
        """Intenta encontrar un registro existente en destino por clave única."""
        try:
            if table == 'account_account':
                cur.execute('SELECT id FROM account_account WHERE code=%s AND company_id=%s',
                            (row_dict.get('code'), row_dict.get('company_id')))
            elif table == 'account_journal':
                cur.execute('SELECT id FROM account_journal WHERE code=%s AND company_id=%s',
                            (row_dict.get('code'), row_dict.get('company_id')))
            elif table == 'account_tax':
                cur.execute('SELECT id FROM account_tax WHERE name=%s AND company_id=%s',
                            (row_dict.get('name'), row_dict.get('company_id')))
            elif table == 'stock_warehouse':
                cur.execute('SELECT id FROM stock_warehouse WHERE code=%s AND company_id=%s',
                            (row_dict.get('code'), row_dict.get('company_id')))
            elif table == 'stock_location':
                # Buscar por complete_name (campo almacenado en DB)
                cname = row_dict.get('complete_name') or row_dict.get('name')
                if cname:
                    cur.execute(
                        'SELECT id FROM stock_location WHERE complete_name=%s LIMIT 1',
                        (cname,)
                    )
                else:
                    return None
            elif table == 'res_partner':
                cur.execute('SELECT id FROM res_partner WHERE name=%s AND company_id IS NOT DISTINCT FROM %s LIMIT 1',
                            (row_dict.get('name'), row_dict.get('company_id')))
            elif table == 'product_category':
                cur.execute('SELECT id FROM product_category WHERE name=%s LIMIT 1',
                            (row_dict.get('name'),))
            else:
                return None

            res = cur.fetchone()
            return res[0] if res else None
        except Exception:
            self.tgt_conn.rollback()
            return None

    def prepare_vals(self, rec: dict, tgt_cols: dict) -> list:
        """
        Retorna la lista de valores con coerción de tipos aplicada.
        Usar en módulos que construyen rec manualmente antes de INSERT.
        """
        return [self._coerce_value(c, v, tgt_cols) for c, v in rec.items()]

    def exec_tgt(self, sql: str, params=None):
        """Ejecuta SQL en destino, retorna filas si las hay."""
        with self.tgt_conn.cursor() as cur:
            cur.execute(sql, params)
            try:
                return cur.fetchall()
            except Exception:
                return []

    def fetch_src(self, sql: str, params=None) -> list:
        """Ejecuta SELECT en origen y retorna lista de DictRow."""
        with self.src_conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(sql, params)
            return cur.fetchall()

    def preload_id_map(self, table: str, match_field: str = 'id'):
        """
        Pre-carga el id_map de una tabla mapeando IDs iguales (útil para tablas
        de configuración como res_currency que no se migran sino que se hacen match).
        """
        if table not in self.id_map:
            self.id_map[table] = {}
        with self.tgt_conn.cursor() as cur:
            cur.execute(f'SELECT id FROM "{table}"')
            for (tgt_id,) in cur.fetchall():
                self.id_map[table][tgt_id] = tgt_id
