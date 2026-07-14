import psycopg2
conn12 = psycopg2.connect(dbname='servitk', user='postgres', password='2010626Ab', host='168.232.165.138', port=5432)
cur12 = conn12.cursor()

conn16 = psycopg2.connect(dbname='clicksale', user='odoo', password='2010', host='localhost', port=5432)
cur16 = conn16.cursor()

print("--- Odoo 12 ---")
cur12.execute("SELECT table_name FROM information_schema.tables WHERE table_name LIKE '%partner%' AND table_name LIKE '%act%'")
print('O12 acteco tables:', cur12.fetchall())

cur12.execute("SELECT column_name FROM information_schema.columns WHERE table_name='res_partner' AND column_name LIKE '%city%'")
print('O12 city columns:', cur12.fetchall())

print("--- Odoo 16 ---")
cur16.execute("SELECT table_name FROM information_schema.tables WHERE table_name LIKE '%partner%' AND table_name LIKE '%act%'")
print('O16 acteco tables:', cur16.fetchall())

cur16.execute("SELECT column_name FROM information_schema.columns WHERE table_name='res_partner' AND column_name LIKE '%city%'")
print('O16 city columns:', cur16.fetchall())

cur12.close()
conn12.close()
cur16.close()
conn16.close()
