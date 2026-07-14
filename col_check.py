import psycopg2
conn12 = psycopg2.connect(dbname='servitk', user='postgres', password='2010626Ab', host='168.232.165.138', port=5432)
cur12 = conn12.cursor()
cur12.execute("SELECT column_name FROM information_schema.columns WHERE table_name='res_partner' AND column_name LIKE '%account%'")
print('O12 columns:', cur12.fetchall())
cur12.close()
conn12.close()
