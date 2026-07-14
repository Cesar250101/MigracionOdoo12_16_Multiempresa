import psycopg2
conn12 = psycopg2.connect(dbname='servitk', user='postgres', password='2010626Ab', host='168.232.165.138', port=5432)
cur12 = conn12.cursor()

conn16 = psycopg2.connect(dbname='clicksale', user='odoo', password='2010', host='localhost', port=5432)
cur16 = conn16.cursor()

cur12.execute("SELECT id FROM res_partner ORDER BY id DESC LIMIT 5")
print('O12 partner IDs:', cur12.fetchall())

cur16.execute("SELECT id FROM res_partner ORDER BY id DESC LIMIT 5")
print('O16 partner IDs:', cur16.fetchall())

cur12.execute("SELECT id, code FROM account_account WHERE company_id=1 ORDER BY id DESC LIMIT 3")
print('O12 accounts:', cur12.fetchall())

cur16.execute("SELECT id, code FROM account_account WHERE company_id=92 ORDER BY id DESC LIMIT 3")
print('O16 accounts:', cur16.fetchall())

cur12.close()
conn12.close()
cur16.close()
conn16.close()
