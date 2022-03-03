import psycopg2
from config import config

def valinta():
    action = int(input("mitä suoritetaan: "))
    if action == 0:
        select_allfromperson()
    elif action == 1:
        show_person_columns()
    elif action == 2:
        show_cert()
    elif action == 3:
        az_cert_owners()
    elif action == 4:
        nimi = input('Anna certin nimi: ')
        personid = int(input('Anna personid: '))
        insert_to_table_certificates(nimi,personid)
    elif action == 5:
        id = int(input('Anna muokattavan rivin id: '))
        name = input('Anna name: ')
        age = input("Anna ikä: ")
        student = input('Onko opiskelija (true,false): ')
        update_person(id, name, age, student)
    elif action == 6:
        id = int(input('Anna muokattavan rivin id: '))
        name = input('Anna certiname: ')
        age = input("Anna hlöid: ")
        update_certificate(id, name, personid)
    elif action == 7:
        table = input('Mistä taulusta poistetaan: ')
        id = int(input('poistettavan rivin id: '))
        delete_fromtable(table,id)

def select_allfromperson():
    con = None
    try:
        con = psycopg2.connect(**config())
        cur = con.cursor()
        SQL = 'SELECT * FROM person;'
        cur.execute(SQL)
        row = cur.fetchone()
        while row is not None:
            print(row)
            row = cur.fetchone()
        cur.close()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if con is not None:
            con.close()

#1
def show_person_columns():
    con = None
    try:
        con = psycopg2.connect(**config())
        cur = con.cursor()
        SQL = 'SELECT * FROM person;'
        cur.execute(SQL)
        colnames = [desc[0] for desc in cur.description]
        print(colnames)
        while row is not None:
            print(row)
            row = cur.fetchone()
        cur.close()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if con is not None:
            con.close()
    

#2
def show_cert():
    con = None
    try:
        con = psycopg2.connect(**config())
        cur = con.cursor()
        SQL = 'SELECT * FROM certificates;'
        cur.execute(SQL)
        colnames = [desc[0] for desc in cur.description]
        print(colnames)
        row = cur.fetchone()
        while row is not None:
            print(row)
            row = cur.fetchone()
        cur.close()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if con is not None:
            con.close()
    
#3
def az_cert_owners():
    con = None
    try:
        con = psycopg2.connect(**config())
        cur = con.cursor()
        SQL = "SELECT person.name, person.id, certificates.name as cert FROM certificates, person WHERE certificates.name = 'AZ-104' AND certificates.person_id = person.id;"
        cur.execute(SQL)
        colnames = [desc[0] for desc in cur.description]
        print(colnames)
        row = cur.fetchone()
        while row is not None:
            print(row)
            row = cur.fetchone()
        cur.close()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if con is not None:
            con.close()
    

#4
def insert_to_table_certificates(nimi, personid):
    con = None
    try:
        con = psycopg2.connect(**config())
        cur = con.cursor()
        SQL = "INSERT INTO certificates (name,person_id) VALUES (%s, %s);"
        data = (nimi,personid)
        cur.execute(SQL, data)
        con.commit()
        cur.close()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if con is not None:
            con.close()
    
def update_person(id, name, age, student):
    con = None
    try:
        con = psycopg2.connect(**config())
        cur = con.cursor()
        SQL = "UPDATE person SET name=%s, age=%s, student=%s WHERE id=%s;"
        data = (name,age,student,id)
        cur.execute(SQL, data)
        con.commit()
        cur.close()
        print('row updated')
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if con is not None:
            con.close()

def update_certificate(id, name, personid):
    con = None
    try:
        con = psycopg2.connect(**config())
        cur = con.cursor()
        SQL = "UPDATE certificate SET name=%s, personid=%s WHERE id=%s;"
        data = (id, name, personid)
        cur.execute(SQL, data)
        con.commit()
        cur.close()        
        print('row updated')
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if con is not None:
            con.close()

def delete_fromtable(table,id):
    con = None
    try:
        con = psycopg2.connect(**config())
        cur = con.cursor()
        SQL = "DELETE FROM %s WHERE id=%s;"
        data = (table,id)
        cur.execute(SQL, data)
        con.commit()
        cur.close()        
        print('row updated')
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if con is not None:
            con.close()

#0. Hae kaikki person taulun rivit ja tulosta ne.
#1. Hae person taulun sarakkeiden nimet ja tulosta ne.
#2. Hae certificate taulun sarakkeiden nimet, sekä rivit ja tulosta ne.
#3. Hae kaikki Azure sertifikaattien omistajat.
#4. Lisää uusi rivi certificate tauluun siten, että arvot otetaan function parametreinä.
if __name__ == '__main__':
    valinta()