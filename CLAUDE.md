# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What this project does

Direct DB-to-DB migration tool from **Odoo 12 to Odoo 16** in a multi-company environment. Uses `psycopg2` for direct PostgreSQL connections — no XML-RPC, no Odoo ORM. Migrates transactional data while handling schema changes between Odoo versions.

## Setup

```bash
pip install -r requirements.txt
```

Edit `config.py` before running: set `SOURCE_DB`, `TARGET_DB`, and `COMPANY_MIGRATION` for your environment.

## Running the migrator

```bash
# Validate connections without migrating
python run.py --dry-run

# Full migration
python run.py

# Run a single step (useful when re-running after a failure)
python run.py --step companies
python run.py --step accounting
python run.py --step stock
python run.py --step sales
python run.py --step purchases
python run.py --step pos
python run.py --step repair
python run.py --step sii_firma
python run.py --step sequences_caf
python run.py --step journal_sii

# Override DB names at runtime
python run.py --src-db odoo12_backup --tgt-db odoo16_test
```

Logs are written to both stdout and `migration.log`.

## Architecture

```
config.py              # DB credentials, company mapping, type maps, flags
run.py                 # CLI entry point; argparse, connection check, step dispatch
migrator_12_16.py      # Migrator12to16: orchestrates all steps in dependency order
core/base_migrator.py  # BaseMigrator: generic table/M2M migration, id_map, type coercion
modules/
  accounting.py        # AccountingMigrator: chart of accounts, taxes, invoices, payments
  sales.py             # SalesMigrator: sale_order + lines, procurement groups
  stock.py             # StockMigrator: locations, warehouses, pickings, moves, quants
  pos.py               # PosMigrator: POS config, sessions, orders, payments
  repair.py            # RepairMigrator: repair orders, lines, fees
```

### Central data structure: `BaseMigrator.id_map`

```python
id_map: dict[str, dict]  # {table_name: {old_id: new_id}}
```

Every inserted record gets its old→new ID registered here. All FK resolution happens through `id_map`. This is the shared state that ties all migrator modules together — modules access it via `self.b.id_map` (where `self.b` is the `BaseMigrator` instance).

### Key migration patterns

**Generic table migration** — `BaseMigrator.migrate_table()`:
- Introspects source and target schemas at runtime via `information_schema.columns`
- Only copies columns that exist in both schemas
- Handles FK remapping via `mapping_fields={field: ref_table}`
- Handles recursive (self-referential) tables via `is_recursive=True`
- Handles jsonb translation fields: Odoo 16 uses `{"en_US": "value"}` where Odoo 12 used plain strings

**M2M tables** — `BaseMigrator.migrate_m2m()`: maps both FK columns through `id_map`.

**Module migrators**: `AccountingMigrator`, `StockMigrator`, etc. receive the `BaseMigrator` instance as `self.b` and perform domain-specific logic (e.g., building `account_tax_repartition_line` rows that don't exist in Odoo 12).

### Critical Odoo 12→16 schema changes

| Domain | Odoo 12 | Odoo 16 |
|--------|---------|---------|
| Invoices | `account_invoice` (separate table) | `account_move` with `move_type` field |
| Account types | `account_account.user_type_id` → `account_account_type` | `account_account.account_type` (selection) |
| Tax accounts | `account_tax.account_id` | `account_tax_repartition_line` rows |
| Stock lots | `stock_production_lot` | `stock_lot` |
| POS payments | `account_bank_statement_line` | `pos_payment` |
| Translatable fields | `varchar` | `jsonb` (`{"en_US": "..."}`) |
| SII CAF | `dte_caf` or `l10n_cl_dte_caf` (auto-detected) | same (auto-detected) |

### Multi-company handling

- `config.py:COMPANY_MIGRATION` maps source company IDs to target company IDs
- `BaseMigrator.map_company(old_id)` translates every `company_id` on insert
- `target_id=0` in the mapping means "create a new company in Odoo 16"
- `DEFAULT_TARGET_COMPANY_ID` is used as fallback for tables without `company_id`

### `config.py` flags that control behavior

- `CLEAN_BEFORE_MIGRATE` — deletes existing data for migrated companies before starting
- `MIGRATE_STOCK_QUANTS` — skip quant migration if only testing other modules
- `MIGRATE_RECONCILIATION` — skip accounting reconciliation for faster partial runs
- `BATCH_SIZE` — rows per batch for bulk inserts
- `LOG_LEVEL` — `DEBUG` for verbose FK/skip warnings, `INFO` for normal runs

## Adding a new module migrator

1. Create `modules/yourmodule.py` with a class that takes `base: BaseMigrator` in `__init__`
2. Use `self.b.migrate_table(...)`, `self.b.fetch_src(...)`, `self.b.id_map`, etc.
3. Import and instantiate in `migrator_12_16.py:Migrator12to16.__init__()`
4. Add calls in `Migrator12to16.run()` in the correct dependency order
5. Add a `--step yourmodule` branch in `run.py:run_step()`
