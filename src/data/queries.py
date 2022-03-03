import psycopg2
from config import config

def connect(action):
    con = None
    try:
        con = psycopg2.connect(**config())
        cur = con.cursor()
        if action == 0 :
            select_all(cur)
        elif action == 1 :
            show_person_columns(cur)
        elif action == 2 :
            show_cert(cur)
        elif action == 3 :
            az_cert_owners(cur)
        elif action == 4 :
            ins(cur)
        cur.close()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if con is not None:
            con.close()

# 0
def select_all(cur):
    SQL = 'SELECT * FROM person;'
    cur.execute(SQL)
    row = cur.fetchone()
    while row is not None:
        print(row)
        row = cur.fetchone()

#1
def show_person_columns(cur):
    SQL = 'SELECT * FROM person;'
    cur.execute(SQL)
    colnames = [desc[0] for desc in cur.description]
    print(colnames)

#2
def show_cert(cur):
    SQL = 'SELECT * FROM certificates;'
    cur.execute(SQL)
    colnames = [desc[0] for desc in cur.description]
    print(colnames)
    row = cur.fetchone()
    while row is not None:
        print(row)
        row = cur.fetchone()
#3
def az_cert_owners(cur):
    SQL = "SELECT person.name, person.id, certificates.name as cert FROM certificates, person WHERE certificates.name = 'AZ-104' AND certificates.person_id = person.id;"
    cur.execute(SQL)
    colnames = [desc[0] for desc in cur.description]
    print(colnames)
    row = cur.fetchone()
    while row is not None:
        print(row)
        row = cur.fetchone()

#4
def insert_to_table_person(cur,value1,value2):
    SQL = "INSERT INTO certificates (name,person_id) VALUES ('AZ-104', 5);"
    cur.execute(SQL, (value1,value2))

#0. Hae kaikki person taulun rivit ja tulosta ne.
#1. Hae person taulun sarakkeiden nimet ja tulosta ne.
#2. Hae certificate taulun sarakkeiden nimet, sekä rivit ja tulosta ne.
#3. Hae kaikki Azure sertifikaattien omistajat.
#4. Lisää uusi rivi certificate tauluun siten, että arvot otetaan function parametreinä.
if __name__ == '__main__':
    connect(1)