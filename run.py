"""
run.py - Punto de entrada para la migración Odoo 12 -> Odoo 16 Multiempresa.

Uso:
    python run.py
    python run.py --step companies       # Solo configurar empresas
    python run.py --step accounting      # Solo contabilidad
    python run.py --step stock           # Solo inventario
    python run.py --step pos             # Solo punto de venta
    python run.py --step sales           # Solo ventas
    python run.py --step purchases       # Solo compras
    python run.py --dry-run              # Validar conexiones sin migrar

Opciones avanzadas:
    python run.py --src-db odoo12_prod   # Override nombre de BD origen
    python run.py --tgt-db odoo16_prod   # Override nombre de BD destino
"""

import argparse
import logging
import sys
import copy

import config as cfg
from migrator_12_16 import Migrator12to16


def setup_logging():
    level = getattr(logging, cfg.LOG_LEVEL, logging.INFO)
    logging.basicConfig(
        level=level,
        format='%(asctime)s %(levelname)-8s %(name)s: %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S',
        handlers=[
            logging.StreamHandler(sys.stdout),
            logging.FileHandler('migration.log', encoding='utf-8'),
        ]
    )


def check_connections(src_db, tgt_db):
    """Verifica que ambas conexiones funcionen antes de empezar."""
    import psycopg2
    log = logging.getLogger('run')

    log.info("Verificando conexión a BD origen (Odoo 12)...")
    try:
        conn = psycopg2.connect(**src_db)
        with conn.cursor() as cur:
            cur.execute("SELECT version(), current_database()")
            ver, db = cur.fetchone()
            log.info("  OK - %s | DB: %s", ver[:50], db)

            # Verificar que es Odoo 12
            cur.execute("""
                SELECT value FROM ir_config_parameter
                WHERE key = 'base.installed.version'
                LIMIT 1
            """)
            row = cur.fetchone()
            if row:
                log.info("  Versión Odoo origen: %s", row[0])
            else:
                log.warning("  No se pudo detectar versión de Odoo en origen.")
        conn.close()
    except Exception as e:
        log.error("FALLO conexión origen: %s", e)
        return False

    log.info("Verificando conexión a BD destino (Odoo 16)...")
    try:
        conn = psycopg2.connect(**tgt_db)
        with conn.cursor() as cur:
            cur.execute("SELECT version(), current_database()")
            ver, db = cur.fetchone()
            log.info("  OK - %s | DB: %s", ver[:50], db)

            cur.execute("""
                SELECT value FROM ir_config_parameter
                WHERE key = 'base.installed.version'
                LIMIT 1
            """)
            row = cur.fetchone()
            if row:
                log.info("  Versión Odoo destino: %s", row[0])
            else:
                log.warning("  No se pudo detectar versión de Odoo en destino.")
        conn.close()
    except Exception as e:
        log.error("FALLO conexión destino: %s", e)
        return False

    # Verificar tabla crítica: account_invoice debe existir en origen (Odoo 12)
    try:
        conn = psycopg2.connect(**src_db)
        with conn.cursor() as cur:
            cur.execute("""
                SELECT EXISTS(
                    SELECT 1 FROM information_schema.tables
                    WHERE table_name='account_invoice' AND table_schema='public'
                )
            """)
            has_invoice = cur.fetchone()[0]
            if has_invoice:
                log.info("  ✓ account_invoice encontrada en origen (confirma Odoo 12).")
            else:
                log.error("  ✗ account_invoice NO encontrada. ¿Es realmente Odoo 12?")
                return False
        conn.close()
    except Exception as e:
        log.error("Error verificando esquema origen: %s", e)
        return False

    return True


def run_full(migrator: Migrator12to16):
    migrator.run()


def run_step(migrator: Migrator12to16, step: str):
    log = logging.getLogger('run')
    log.info("Ejecutando paso: %s", step)

    if step == 'companies':
        migrator.setup_companies()
    elif step == 'accounting':
        migrator.migrate_base_config()
        migrator.accounting.migrate_chart_of_accounts()
        migrator.accounting.migrate_taxes()
        migrator.accounting.migrate_journals()
        migrator.accounting.migrate_invoices()
        migrator.accounting.migrate_journal_entries()
        migrator.accounting.migrate_move_lines()
        migrator.base.migrate_m2m(
            'account_move_line_account_tax_rel',
            'account_move_line_id', 'account_tax_id',
            'account_move_line', 'account_tax'
        )
        # Líneas de producto de facturas (requiere productos/partners ya mapeados)
        migrator.accounting.migrate_invoice_lines()
        migrator.accounting.migrate_payments()
        migrator.accounting.post_migration_updates()
    elif step == 'stock':
        migrator.stock.migrate_locations()
        migrator.stock.migrate_warehouses()
        migrator.stock.migrate_picking_types()
        migrator.stock.migrate_routes()
        migrator.stock.migrate_lots()
        migrator.stock.migrate_pickings()
        migrator.stock.migrate_moves()
        migrator.stock.migrate_move_lines()
        if cfg.MIGRATE_STOCK_QUANTS:
            migrator.stock.migrate_quants()
        migrator.stock.post_migration_stock()
    elif step == 'pos':
        migrator.pos.migrate_payment_methods()
        migrator.pos.migrate_config()
        migrator.pos.migrate_sessions()
        migrator.pos.migrate_orders()
        migrator.pos.migrate_order_lines()
        migrator.pos.migrate_pos_payments()
    elif step == 'sales':
        migrator.sales.migrate_sales()
        # Vinculación sale_order_line <-> invoice lines (si la contabilidad ya fue migrada)
        migrator.base.migrate_m2m(
            'sale_order_line_invoice_rel',
            'order_line_id', 'invoice_line_id',
            'sale_order_line', 'account_invoice_line'
        )
    elif step == 'purchases':
        migrator.migrate_purchases()
    else:
        log.error("Paso desconocido: %s", step)
        sys.exit(1)

    migrator.base.update_sequences()
    migrator.base.fix_ir_sequences()


def main():
    setup_logging()
    log = logging.getLogger('run')

    parser = argparse.ArgumentParser(
        description='Migración Odoo 12 -> Odoo 16 Multiempresa (DB directa, sin XML-RPC)'
    )
    parser.add_argument(
        '--step',
        choices=['companies', 'accounting', 'stock', 'pos', 'sales', 'purchases'],
        help='Ejecutar solo un paso específico de la migración'
    )
    parser.add_argument(
        '--dry-run',
        action='store_true',
        help='Solo verificar conexiones sin ejecutar la migración'
    )
    parser.add_argument('--src-db', help='Override del nombre de BD origen')
    parser.add_argument('--tgt-db', help='Override del nombre de BD destino')
    args = parser.parse_args()

    # Aplicar overrides de BD
    source_db = copy.deepcopy(cfg.SOURCE_DB)
    target_db = copy.deepcopy(cfg.TARGET_DB)
    if args.src_db:
        source_db['dbname'] = args.src_db
    if args.tgt_db:
        target_db['dbname'] = args.tgt_db

    # Verificar conexiones
    if not check_connections(source_db, target_db):
        log.error("Verificación de conexiones fallida. Abortando.")
        sys.exit(1)

    if args.dry_run:
        log.info("--dry-run: conexiones OK. No se ejecutó migración.")
        return

    # Confirmar antes de proceder
    log.warning("=" * 60)
    log.warning("ADVERTENCIA: Esta operación modificará la BD destino.")
    log.warning("  Origen: %s@%s/%s", source_db['user'], source_db['host'], source_db['dbname'])
    log.warning("  Destino: %s@%s/%s", target_db['user'], target_db['host'], target_db['dbname'])
    log.warning("=" * 60)

    # Crear migrador
    migrator = Migrator12to16(
        source_db=source_db,
        target_db=target_db,
        company_migration=cfg.COMPANY_MIGRATION,
    )

    if args.step:
        run_step(migrator, args.step)
    else:
        run_full(migrator)


if __name__ == '__main__':
    main()
