"""diag_caf.py - Diagnóstico del estado de la tabla dte_caf en origen y destino."""
import psycopg2
import psycopg2.extras
import config as cfg

def get_table(conn, candidates):
    with conn.cursor() as cur:
        for t in candidates:
            cur.execute(
                "SELECT EXISTS(SELECT 1 FROM information_schema.tables "
                "WHERE table_name=%s AND table_schema='public')", (t,)
            )
            if cur.fetchone()[0]:
                return t
    return None

def get_columns(conn, table):
    with conn.cursor() as cur:
        cur.execute(
            "SELECT column_name, data_type FROM information_schema.columns "
            "WHERE table_name=%s AND table_schema='public' ORDER BY ordinal_position",
            (table,)
        )
        return [(r[0], r[1]) for r in cur.fetchall()]

src_conn = psycopg2.connect(**cfg.SOURCE_DB)
tgt_conn = psycopg2.connect(**cfg.TARGET_DB)

src_table = get_table(src_conn, ('dte_caf', 'l10n_cl_dte_caf'))
tgt_table = get_table(tgt_conn, ('dte_caf', 'l10n_cl_dte_caf'))

print(f"\n=== TABLA ORIGEN: {src_table}  |  TABLA DESTINO: {tgt_table} ===")

# ── Columnas destino
tgt_cols = get_columns(tgt_conn, tgt_table)
print("\nColumnas destino:")
for c, t in tgt_cols:
    print(f"  {c:35s} {t}")

# ── Registros destino con campos críticos
print("\nRegistros CAF en destino:")
with tgt_conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
    has_caf_file = any(c == 'caf_file' for c, _ in tgt_cols)
    caf_col = ", (caf_file IS NULL) AS caf_null" if has_caf_file else ""
    cur.execute(
        f"SELECT id, company_id, document_class_id, state, "
        f"start_nm, final_nm {caf_col} FROM {tgt_table} ORDER BY id"
    )
    rows = cur.fetchall()
    if not rows:
        print("  (sin registros)")
    for r in rows:
        print(f"  id={r['id']:5d}  company={r['company_id']}  "
              f"doc_class={r['document_class_id']}  "
              f"state={str(r['state']):10s}  "
              f"start={r['start_nm']}  final={r['final_nm']}"
              + (f"  caf_null={r['caf_null']}" if has_caf_file else ""))

# ── Registros origen con campos críticos
src_cols = get_columns(src_conn, src_table)
src_col_names = [c for c, _ in src_cols]
has_src_caf = 'caf_file' in src_col_names
print(f"\nRegistros CAF en origen (tabla={src_table}):")
with src_conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
    src_caf_col = ", (caf_file IS NULL) AS caf_null" if has_src_caf else ""
    cur.execute(
        f"SELECT id, company_id, sii_document_class, status, "
        f"start_nm, final_nm {src_caf_col} FROM {src_table} ORDER BY id LIMIT 5"
    )
    rows = cur.fetchall()
    for r in rows:
        print(f"  id={r['id']:5d}  company={r['company_id']}  "
              f"doc_class={r['sii_document_class']}  "
              f"status={str(r['status']):10s}  "
              f"start={r['start_nm']}  final={r['final_nm']}"
              + (f"  caf_null={r['caf_null']}" if has_src_caf else ""))

src_conn.close()
tgt_conn.close()
