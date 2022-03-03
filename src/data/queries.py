import psycopg2
from config import config

def connect():
    con = None
    try:
        con = psycopg2.connect(**config())
        cur = con.cursor()
        valinta(con,cur)
        con.commit()
        cur.close()        
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if con is not None:
            con.close()

def select_allfromperson(cur):
    SQL = 'SELECT * FROM person;'
    cur.execute(SQL)
    row = cur.fetchone()
    while row is not None:
        print(row)
        row = cur.fetchone()

def show_person_columns(cur):
    SQL = 'SELECT * FROM person;'
    cur.execute(SQL)
    colnames = [desc[0] for desc in cur.description]
    print(colnames)
    while row is not None:
        print(row)
        row = cur.fetchone()
        
def show_cert(cur):
    SQL = 'SELECT * FROM certificates;'
    cur.execute(SQL)
    colnames = [desc[0] for desc in cur.description]
    print(colnames)
    row = cur.fetchone()
    while row is not None:
        print(row)
        row = cur.fetchone()
    
def az_cert_owners(cur):
    SQL = "SELECT person.name, person.id, certificates.name as cert FROM certificates, person WHERE certificates.name = 'AZ-104' AND certificates.person_id = person.id;"
    cur.execute(SQL)
    colnames = [desc[0] for desc in cur.description]
    print(colnames)
    row = cur.fetchone()
    while row is not None:
        print(row)
        row = cur.fetchone()

def insert_to_table_certificates(con,cur,nimi, personid):
    SQL = "INSERT INTO certificates (name,person_id) VALUES (%s, %s);"
    data = (nimi,personid)
    cur.execute(SQL, data)
    con.commit()
    
def update_person(con,cur,id, name, age, student):
    SQL = "UPDATE person SET name=%s, age=%s, student=%s WHERE id=%s;"
    data = (name,age,student,id)
    cur.execute(SQL, data)
    con.commit()
    
    print('row updated')


def update_certificate(con,cur,id, name, personid):
    SQL = "UPDATE certificates SET name=%s, person_id=%s WHERE id=%s;"
    data = (name, personid, id)
    cur.execute(SQL, data)
    con.commit()
            
    print('row updated')

def delete_fromcertificates(con,cur,id):
    SQL = "DELETE FROM certificates WHERE id=%s;"
    data = (id,)
    cur.execute(SQL, data)
    con.commit()
            
    print('row updated')

def delete_fromperson(con,cur,id):
        con = psycopg2.connect(**config())
        cur = con.cursor()
        SQL = "DELETE FROM person WHERE id=%s;"
        data = (id,)
        cur.execute(SQL, data)
        con.commit()
                
        print('row updated')

def create_tables(con,cur):
    commands = (
        """
        CREATE TABLE rahaa (
            tili_id SERIAL PRIMARY KEY,
            tili_omistajaname VARCHAR(255) NOT NULL
            tili_rahaa INT
        )
        """,)

    for command in commands:
        cur.execute(command)

    con.commit()

def valinta(con,cur):
    print('0: select_allfromperson()')
    print('1: show_person_columns()')
    print('2: show_cert()')
    print('3: az_cert_owners()')
    print('4: insert_to_table_certificates(nimi,personid)')
    print('5: update_person(id, name, age, student)')
    print('6: update_certificate(id, name, personid)')
    print('7: delete_fromcertificates(id)')
    print('8: delete_fromperson')
    print('9: create tables')
    action = int(input("mitä suoritetaan: "))
    if action == 0:
        select_allfromperson(cur)
    elif action == 1:
        show_person_columns(cur)
    elif action == 2:
        show_cert(cur)
    elif action == 3:
        az_cert_owners(cur)
    elif action == 4:
        select_allfromperson(cur)
        nimi = input('Anna certin nimi: ')
        personid = int(input('Anna personid: '))
        insert_to_table_certificates(con,cur,nimi,personid)
    elif action == 5:
        select_allfromperson(cur)
        id = int(input('Anna muokattavan rivin id: '))
        name = input('Anna name: ')
        age = input("Anna ikä: ")
        student = input('Onko opiskelija (true,false): ')
        update_person(con,cur,id, name, age, student)
    elif action == 6:
        show_cert(cur)
        id = int(input('Anna muokattavan rivin id: '))
        name = input('Anna certiname: ')
        personid = int(input("Anna hlöid: "))
        update_certificate(con,cur,id, name, personid)
    elif action == 7:
        show_cert(cur)
        id = int(input('poistettavan rivin id: '))
        delete_fromcertificates(con,cur,id)
    elif action == 8:
        select_allfromperson(cur)
        id = int(input('poistettavan rivin id: '))
        delete_fromperson(con,cur,id)
    elif action == 9:
        create_tables(con,cur)

""" 0: select_allfromperson()
    1: show_person_columns()
    2: show_cert()
    3: az_cert_owners()
    4: insert_to_table_certificates(nimi,personid)
    5: update_person(id, name, age, student)
    6: update_certificate(id, name, personid)
    7: delete_fromcertificates(id)
    8: delete_fromperson(id)
    9: create_tables() """
    
if __name__ == '__main__':
    connect()