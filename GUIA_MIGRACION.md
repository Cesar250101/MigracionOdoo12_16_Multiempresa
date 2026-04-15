# Guía de Migración Odoo 12 → Odoo 16 Multiempresa

## Prerrequisitos

```bash
pip install -r requirements.txt
```

## Configuración

Editar `config.py` antes de ejecutar:

```python
SOURCE_DB = {
    'dbname': 'tu_bd_odoo12',
    'user': 'postgres',
    'password': '...',
    'host': 'servidor-origen',
    'port': '5432',
}

TARGET_DB = {
    'dbname': 'tu_bd_odoo16',
    'user': 'postgres',
    'password': '...',
    'host': 'localhost',
    'port': '5432',
}

COMPANY_MIGRATION = [
    {'source_id': 1, 'target_id': 1, 'name': 'Empresa Principal'},
    {'source_id': 2, 'target_id': 0, 'name': 'Sucursal Norte'},  # target_id=0 = crear nueva
]
```

## Uso

```bash
# Verificar conexiones (sin migrar)
python run.py --dry-run

# Migración completa
python run.py

# Pasos individuales (útil para re-ejecutar si algo falla)
python run.py --step companies    # Solo empresas
python run.py --step accounting   # Solo contabilidad
python run.py --step stock        # Solo inventario
python run.py --step sales        # Solo ventas
python run.py --step purchases    # Solo compras
python run.py --step pos          # Solo punto de venta

# Override de nombre de BD
python run.py --src-db odoo12_backup --tgt-db odoo16_test
```

## Transformaciones clave Odoo 12 → 16

### Contabilidad (cambio más crítico)

| Odoo 12 | Odoo 16 |
|---------|---------|
| `account_invoice` (tabla separada) | Integrada en `account_move` como `move_type` |
| `account_invoice.type` | `account_move.move_type` |
| `account_invoice.date_invoice` | `account_move.invoice_date` |
| `account_invoice.date_due` | `account_move.invoice_date_due` |
| `account_invoice.reference` | `account_move.ref` |
| `account_invoice.origin` | `account_move.invoice_origin` |
| `account_invoice.payment_term_id` | `account_move.invoice_payment_term_id` |
| `account_account.user_type_id` (M2O) | `account_account.account_type` (selection) |
| `account_tax.account_id` | `account_tax_repartition_line` con `repartition_type='tax'` |
| `account_invoice.state='open'` | `account_move.state='posted'` |

### Inventario

| Odoo 12 | Odoo 16 |
|---------|---------|
| `stock.production.lot` → tabla `stock_production_lot` | `stock.lot` → tabla `stock_lot` |
| `stock_quant` con múltiples registros por (product+location+lot) y `reservation_id` | `stock_quant` consolidado con `reserved_quantity` |

### Punto de Venta

| Odoo 12 | Odoo 16 |
|---------|---------|
| `pos_config.journal_ids` (M2M con `account_journal`) | `pos_config.payment_method_ids` → `pos_payment_method` |
| Pagos en `account_bank_statement_line` | Pagos en `pos_payment` |

## Orden de migración

```
1.  Empresas (multiempresa)
2.  Monedas (mapeo por nombre ISO)
3.  Condiciones de pago / Posiciones fiscales / Categorías
4.  Contactos (res_partner)
5.  Plan de cuentas → account_type mapeado
6.  Impuestos → repartition lines creadas automáticamente
7.  Diarios
8.  Secuencias
9.  Ubicaciones → Almacenes → Tipos operación → Rutas/Reglas
10. Productos
11. Ventas (sale_order + lines)
12. Albaranes + movimientos + lotes + quants
13. Compras (purchase_order + lines)
14. Facturas (account_invoice → account_move)
15. Asientos contables puros (account_move 'entry')
16. Conciliaciones completas
17. Líneas contables (account_move_line)
18. Pagos (account_payment)
19. Conciliaciones parciales
20. POS (config → sesiones → órdenes → pagos)
21. Post-migración: vinculaciones circulares
22. Actualización de secuencias PostgreSQL
```

## Notas importantes

### Multiempresa
- Todos los registros con `company_id` se reasignan al `target_id` configurado.
- Si `target_id = 0`, se crea la empresa automáticamente en Odoo 16.
- Las empresas deben tener su plan de cuentas instalado en Odoo 16 antes de migrar.

### Facturas
- Las facturas en estado `open`, `in_payment` o `paid` en Odoo 12 quedan como `posted` en Odoo 16.
- El campo `payment_state` se infiere del estado original de la factura.
- Revisar facturas con reconciliaciones complejas manualmente.

### Stock
- Los lotes/series se migran de `stock_production_lot` → `stock_lot`.
- Las existencias se consolidan: múltiples quants por (product, location, lot) en Odoo 12
  se unen en un solo quant con `quantity` y `reserved_quantity` correctos.

### Módulos no migrados
Los siguientes módulos no están incluidos (requieren customización adicional):
- Manufactura (`mrp`)
- e-Commerce (`website_sale`)
- Firma electrónica SII (Chile)
- Módulos de terceros

## Solución de problemas

**Error: `account_invoice` no encontrada en origen**
→ Verificar que la BD origen realmente es Odoo 12.

**Error en `account_tax_repartition_line` NOT NULL**
→ Actualizar Odoo 16 con `python -m odoo -d DB -u account --stop-after-init` antes de migrar.

**FK violations en `account_move_line.tax_repartition_line_id`**
→ Ejecutar primero `--step accounting` para crear los impuestos y sus repartition lines.

**Secuencias rotas en Odoo 16 post-migración**
→ El script ejecuta `fix_ir_sequences()` automáticamente al finalizar.
→ Si persiste: `python -m odoo -d DB -u base --stop-after-init`

**Log de migración**
El archivo `migration.log` en el directorio del script contiene el historial completo.
