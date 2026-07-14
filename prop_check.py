import psycopg2
import json

conn12 = psycopg2.connect(dbname='servitk', user='postgres', password='2010626Ab', host='168.232.165.138', port=5432)
cur12 = conn12.cursor()

conn16 = psycopg2.connect(dbname='clicksale', user='odoo', password='2010', host='localhost', port=5432)
cur16 = conn16.cursor()

cur12.execute("""
SELECT name, res_id, value_reference 
FROM ir_property 
WHERE name IN ('property_account_payable_id', 'property_account_receivable_id') 
  AND res_id LIKE 'res.partner,%'
LIMIT 5
""")
print("O12 properties:", cur12.fetchall())

cur16.execute("""
SELECT name, res_id, value_reference 
FROM ir_property 
WHERE name IN ('property_account_payable_id', 'property_account_receivable_id') 
  AND res_id LIKE 'res.partner,%'
LIMIT 5
""")
print("O16 properties:", cur16.fetchall())

# Check how acteco is mapped
cur12.execute("SELECT res_partner_id, partner_activities_id FROM partner_activities_res_partner_rel LIMIT 3")
print("O12 acteco rel:", cur12.fetchall())

cur16.execute("SELECT res_partner_id, partner_activities_id FROM partner_activities_res_partner_rel LIMIT 3")
print("O16 acteco rel:", cur16.fetchall())

cur12.close()
conn12.close()
cur16.close()
conn16.close()
