import psycopg2
conn12 = psycopg2.connect(dbname='servitk', user='postgres', password='2010626Ab', host='168.232.165.138', port=5432)
cur12 = conn12.cursor()

conn16 = psycopg2.connect(dbname='clicksale', user='odoo', password='2010', host='localhost', port=5432)
cur16 = conn16.cursor()

# Get O12 partners (with their acteco from many2many, city_id, and vat)
# In O12, vat is often called vat or document_number
try:
    cur12.execute("SELECT column_name FROM information_schema.columns WHERE table_name='res_partner' AND column_name IN ('vat', 'document_number')")
    print("O12 vat cols:", cur12.fetchall())
except Exception as e:
    print(e)

try:
    cur16.execute("SELECT column_name FROM information_schema.columns WHERE table_name='res_partner' AND column_name IN ('vat', 'document_number')")
    print("O16 vat cols:", cur16.fetchall())
except Exception as e:
    print(e)
