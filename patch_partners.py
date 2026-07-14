import psycopg2
from config import SOURCE_DB, TARGET_DB

def get_conn(db_cfg):
    return psycopg2.connect(
        dbname=db_cfg['dbname'],
        user=db_cfg['user'],
        password=db_cfg['password'],
        host=db_cfg['host'],
        port=db_cfg['port']
    )

def main():
    src_conn = get_conn(SOURCE_DB)
    tgt_conn = get_conn(TARGET_DB)

    src_cur = src_conn.cursor()
    tgt_cur = tgt_conn.cursor()

    tgt_company_id = 92
    src_company_id = 1

    print("Leyendo partners de destino...")
    tgt_cur.execute("SELECT id, document_number, name FROM res_partner WHERE company_id = %s OR company_id IS NULL", (tgt_company_id,))
    tgt_partners = tgt_cur.fetchall()
    tgt_by_vat = {r[1]: r[0] for r in tgt_partners if r[1]}
    tgt_by_name = {r[2]: r[0] for r in tgt_partners if r[2]}

    print("Leyendo partners de origen...")
    src_cur.execute("SELECT id, document_number, name, city_id FROM res_partner WHERE company_id = %s OR company_id IS NULL", (src_company_id,))
    src_partners = src_cur.fetchall()

    partner_id_map = {}
    for sp in src_partners:
        sp_id, sp_vat, sp_name, sp_city = sp
        tgt_id = tgt_by_vat.get(sp_vat) or tgt_by_name.get(sp_name)
        if tgt_id:
            partner_id_map[sp_id] = tgt_id

    print(f"Mapeados {len(partner_id_map)} partners entre Odoo 12 y 16.")

    # 1. Migrar city_id
    try:
        src_cur.execute("SELECT id, name FROM res_city")
        src_cities = {}
        for r in src_cur.fetchall():
            name = r[1]
            if isinstance(name, dict):
                name = name.get('es_CL', name.get('en_US', str(name)))
            src_cities[r[0]] = name

        tgt_cur.execute("SELECT id, name FROM res_city")
        tgt_city_by_name = {}
        for r in tgt_cur.fetchall():
            name = r[1]
            if isinstance(name, dict):
                name = name.get('es_CL', name.get('en_US', str(name)))
            tgt_city_by_name[name] = r[0]

        city_updates = 0
        for sp in src_partners:
            sp_id, sp_vat, sp_name, sp_city = sp
            tgt_id = partner_id_map.get(sp_id)
            if tgt_id and sp_city:
                city_name = src_cities.get(sp_city)
                tgt_city_id = tgt_city_by_name.get(city_name)
                if tgt_city_id:
                    tgt_cur.execute("UPDATE res_partner SET city_id = %s WHERE id = %s", (tgt_city_id, tgt_id))
                    city_updates += 1
        print(f"city_id actualizados: {city_updates}")
    except Exception as e:
        tgt_conn.rollback()
        print(f"Error en city_id: {e}")

    # 2. Migrar acteco_ids
    try:
        src_cur.execute("SELECT res_partner_id, partner_activities_id FROM partner_activities_res_partner_rel")
        src_actecos = src_cur.fetchall()

        src_cur.execute("SELECT id, code FROM partner_activities")
        src_acteco_codes = {r[0]: r[1] for r in src_cur.fetchall()}
        tgt_cur.execute("SELECT id, code FROM partner_activities")
        tgt_acteco_by_code = {r[1]: r[0] for r in tgt_cur.fetchall()}

        acteco_inserts = 0
        for rel in src_actecos:
            sp_id, s_act_id = rel
            tgt_id = partner_id_map.get(sp_id)
            if tgt_id:
                code = src_acteco_codes.get(s_act_id)
                tgt_act_id = tgt_acteco_by_code.get(code)
                if tgt_act_id:
                    tgt_cur.execute("SELECT 1 FROM partner_activities_res_partner_rel WHERE res_partner_id=%s AND partner_activities_id=%s", (tgt_id, tgt_act_id))
                    if not tgt_cur.fetchone():
                        tgt_cur.execute("INSERT INTO partner_activities_res_partner_rel (res_partner_id, partner_activities_id) VALUES (%s, %s)", (tgt_id, tgt_act_id))
                        acteco_inserts += 1
        print(f"acteco_ids (actividades) insertados: {acteco_inserts}")
    except Exception as e:
        tgt_conn.rollback()
        print(f"Error en acteco_ids: {e}")

    # 3. Migrar properties
    try:
        src_cur.execute("SELECT name, res_id, value_reference FROM ir_property WHERE name IN ('property_account_payable_id', 'property_account_receivable_id') AND (res_id LIKE 'res.partner,%' OR res_id IS NULL)")
        src_props = src_cur.fetchall()

        src_cur.execute("SELECT id, code FROM account_account")
        src_account_codes = {str(r[0]): r[1] for r in src_cur.fetchall()}
        tgt_cur.execute("SELECT id, code FROM account_account WHERE company_id=%s", (tgt_company_id,))
        tgt_account_by_code = {r[1]: str(r[0]) for r in tgt_cur.fetchall()}

        # Buscar el id de los fields property_account_payable_id y property_account_receivable_id en Odoo 16 para setear fields_id (requerido a veces)
        tgt_cur.execute("SELECT id, name FROM ir_model_fields WHERE name IN ('property_account_payable_id', 'property_account_receivable_id') AND model='res.partner'")
        tgt_fields = {r[1]: r[0] for r in tgt_cur.fetchall()}

        prop_updates = 0
        for prop in src_props:
            p_name, p_resid, p_valref = prop
            if p_resid is None:
                tgt_id = None
                tgt_resid = None
            else:
                sp_id = int(p_resid.split(',')[1])
                tgt_id = partner_id_map.get(sp_id)
                if not tgt_id:
                    continue
                tgt_resid = f"res.partner,{tgt_id}"
                
            if p_valref:
                s_acc_id = p_valref.split(',')[1]
                acc_code = src_account_codes.get(s_acc_id)
                tgt_acc_id = tgt_account_by_code.get(acc_code)
                
                if tgt_acc_id:
                    tgt_valref = f"account.account,{tgt_acc_id}"
                    field_id = tgt_fields.get(p_name)
                    
                    if tgt_resid:
                        tgt_cur.execute("SELECT id FROM ir_property WHERE name=%s AND res_id=%s AND company_id=%s", (p_name, tgt_resid, tgt_company_id))
                        if tgt_cur.fetchone():
                            tgt_cur.execute("UPDATE ir_property SET value_reference=%s WHERE name=%s AND res_id=%s AND company_id=%s", (tgt_valref, p_name, tgt_resid, tgt_company_id))
                        else:
                            tgt_cur.execute("INSERT INTO ir_property (name, res_id, value_reference, company_id, type, fields_id) VALUES (%s, %s, %s, %s, 'many2one', %s)", (p_name, tgt_resid, tgt_valref, tgt_company_id, field_id))
                    else:
                        tgt_cur.execute("SELECT id FROM ir_property WHERE name=%s AND res_id IS NULL AND company_id=%s", (p_name, tgt_company_id))
                        if tgt_cur.fetchone():
                            tgt_cur.execute("UPDATE ir_property SET value_reference=%s WHERE name=%s AND res_id IS NULL AND company_id=%s", (tgt_valref, p_name, tgt_company_id))
                        else:
                            tgt_cur.execute("INSERT INTO ir_property (name, res_id, value_reference, company_id, type, fields_id) VALUES (%s, NULL, %s, %s, 'many2one', %s)", (p_name, tgt_valref, tgt_company_id, field_id))
                    
                    prop_updates += 1
        print(f"Cuentas contables (properties) actualizadas: {prop_updates}")
    except Exception as e:
        tgt_conn.rollback()
        print(f"Error en properties: {e}")

    tgt_conn.commit()
    print("Parche completado exitosamente.")

    src_cur.close()
    tgt_cur.close()
    src_conn.close()
    tgt_conn.close()

if __name__ == '__main__':
    main()
