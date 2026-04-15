"""
config.py - Configuración de conexiones y mapeo de empresas
Migración Odoo 12 -> Odoo 16 Multiempresa
"""

# ─────────────────────────────────────────────
# CONFIGURACIÓN BASE DE DATOS ORIGEN (Odoo 12)
# ─────────────────────────────────────────────
SOURCE_DB = {
    'dbname': 'grupoqualitas',
    'user': 'postgres',
    'password': '2010626Ab',
    'host': '168.232.165.138',
    'port': '5432',
}

# ─────────────────────────────────────────────
# CONFIGURACIÓN BASE DE DATOS DESTINO (Odoo 16)
# ─────────────────────────────────────────────
TARGET_DB = {
    'dbname': 'clicksale',
    'user': 'odoo',
    'password': '2010',
    'host': 'localhost',
    'port': '5432',
}

# ─────────────────────────────────────────────
# CONFIGURACIÓN MULTIEMPRESA
# ─────────────────────────────────────────────
# Lista de empresas a migrar.
# Formato: {'source_id': ID en Odoo 12, 'target_id': ID en Odoo 16 (0 = crear nueva)}
# Si target_id = 0, la empresa se creará automáticamente en Odoo 16.
COMPANY_MIGRATION = [
    {'source_id': 1, 'target_id': 67, 'name': 'Empresa Principal'},
    # {'source_id': 2, 'target_id': 0, 'name': 'Sucursal Norte'},
]

# Empresa fallback si no hay mapeo definido (para tablas sin company_id)
DEFAULT_TARGET_COMPANY_ID = 67

# ─────────────────────────────────────────────
# OPCIONES DE MIGRACIÓN
# ─────────────────────────────────────────────
BATCH_SIZE = 500          # Registros por lote en inserciones masivas
CLEAN_BEFORE_MIGRATE = True   # Limpiar datos previos del destino para las empresas migradas
MIGRATE_STOCK_QUANTS = True   # Migrar cantidades en stock (stock_quant)
MIGRATE_RECONCILIATION = True # Migrar conciliaciones contables
LOG_LEVEL = 'INFO'            # DEBUG | INFO | WARNING | ERROR

# ─────────────────────────────────────────────
# MAPEO account_type: Odoo 12 internal_type + internal_group -> Odoo 16 account_type
# ─────────────────────────────────────────────
ACCOUNT_TYPE_MAP = {
    # (internal_type, internal_group) -> account_type Odoo 16
    ('receivable', 'asset'):    'asset_receivable',
    ('payable', 'liability'):   'liability_payable',
    ('liquidity', 'asset'):     'asset_cash',
    ('liquidity', 'liability'): 'asset_cash',
    ('other', 'asset'):         'asset_current',
    ('other', 'liability'):     'liability_current',
    ('other', 'equity'):        'equity',
    ('other', 'income'):        'income',
    ('other', 'expense'):       'expense',
    # Fallbacks por internal_type solo
    ('receivable', None):       'asset_receivable',
    ('payable', None):          'liability_payable',
    ('liquidity', None):        'asset_cash',
    ('other', None):            'asset_current',
}

ACCOUNT_TYPE_FALLBACK = 'asset_current'

# ─────────────────────────────────────────────
# MAPEO ESTADOS FACTURA: Odoo 12 -> Odoo 16
# ─────────────────────────────────────────────
INVOICE_STATE_MAP = {
    'draft':      'draft',
    'open':       'posted',
    'in_payment': 'posted',
    'paid':       'posted',
    'cancel':     'cancel',
}

INVOICE_PAYMENT_STATE_MAP = {
    'draft':      'not_paid',
    'open':       'not_paid',
    'in_payment': 'in_payment',
    'paid':       'paid',
    'cancel':     'not_paid',
}

# ─────────────────────────────────────────────
# TABLAS A LIMPIAR ANTES DE MIGRAR (orden inverso de FK)
# ─────────────────────────────────────────────
TABLES_TO_CLEAN = [
    'account_partial_reconcile',
    'account_full_reconcile',
    'account_payment',
    'account_move_line',
    'account_move',
    'pos_payment',
    'pos_order_line',
    'pos_order',
    'pos_session',
    'purchase_order_line',
    'purchase_order',
    'sale_order_line',
    'sale_order',
    'stock_move_line',
    'stock_move',
    'stock_picking',
    'stock_quant',
    'stock_lot',
    'stock_rule',
    'stock_route',
    'stock_picking_type',
    'stock_warehouse',
    'stock_location',
    'ir_sequence_date_range',
    'ir_sequence',
    'dte_caf',
    'l10n_cl_dte_caf',
    'sii_firma',
    'l10n_cl_certificate',
    'account_tax_repartition_line',
    'account_tax',
    'account_journal',
    'account_account',
    'account_payment_term',
    'account_fiscal_position',
    'product_product',
    'product_template',
    'product_category',
    'res_partner',
]
