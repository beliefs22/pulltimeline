import sqlite3


def makeTable(subject_file):
    """Creates a database for the given subject"""
    print("Creating database for file {}".format(subject_file))
    conn = sqlite3.connect(subject_file.replace(".txt",".db"))
    cur = conn.cursor()
    # dispotion time table
    cur.execute('DROP TABLE IF EXISTS disposition')
    cur.execute('CREATE TABLE disposition (arrival_date,arrival_time,\
    departure_date,departure_time,disposition)')

    # vitals table
    cur.execute('DROP TABLE IF EXISTS vitals')
    cur.execute('CREATE TABLE vitals (time,bp,temp,rate,resp,spo2,\
    pain_score,device,rate_lmin)')

    # cbc table
    cur.execute('DROP TABLE IF EXISTS cbc')
    cur.execute('CREATE TABLE cbc (time,result_time,white_blood_cell_count,\
    red_blood_cell_count,hemoglobin,hematocrit,platelet_count)')

    # cmp table
    cur.execute('DROP TABLE IF EXISTS cmp')
    cur.execute('CREATE TABLE cmp (time,result_time,sodium,potassium,chloride,\
    carbon_dioxide,urea_nitrogen,creatinine_serum,glucose,albumin,bilirubin_total)')

    # influenza test table
    cur.execute('DROP TABLE IF EXISTS influenza')
    cur.execute('CREATE TABLE influenza (time,result_time, influenza_a_nat,\
    influenza_b_nat,rsv_nat)')

    # Resp Virus Test table
    cur.execute('DROP TABLE IF EXISTS respvirus')
    cur.execute('CREATE TABLE respvirus (time, type)')

    # medication table
    cur.execute('DROP TABLE IF EXISTS medications')
    cur.execute('CREATE TABLE medications (time, name)')

    # assessment table
    cur.execute('DROP TABLE IF EXISTS assessments')
    cur.execute('CREATE TABLE assessments (time,glasgow_coma_scale_score)')

    # blood gass table
    cur.execute('DROP TABLE IF EXISTS bloodgas')
    cur.execute('CREATE TABLE bloodgas (time, result_time, ph_arterial,\
    pco2_arterial, po2_arterial,ph_non_arterial,pco2_non_arterial,\
    po2_non_arterial)')

    # imaging table
    cur.execute('DROP TABLE IF EXISTS imaging')
    cur.execute('CREATE TABLE imaging (time, type)')

    # culture table
    cur.execute('DROP TABLE IF EXISTS cultures')
    cur.execute('CREATE TABLE cultures (time, type)')

    # xpert table
    cur.execute('DROP TABLE IF EXISTS xpert')
    cur.execute('CREATE TABLE xpert (time, type)')
    conn.commit()

    return conn
    
def main():
    pass
    


if __name__ == "__main__":
    main()  
