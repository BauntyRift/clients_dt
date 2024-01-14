import psycopg2

def create_db(conn):
    with conn.cursor() as cur:
        cur.execute('''
            CREATE TABLE IF NOT EXISTS clients (
                id SERIAL PRIMARY KEY,
                first_name TEXT,
                last_name TEXT,
                email TEXT,
                phones TEXT[]
            )
        ''')
    conn.commit()

def add_client(conn, first_name, last_name, email, phones=None):
    with conn.cursor() as cur:
        cur.execute('INSERT INTO clients (first_name, last_name, email, phones) VALUES (%s, %s, %s, %s)', (first_name, last_name, email, phones))
    conn.commit()

def add_phone(conn, client_id, phone):
    with conn.cursor() as cur:
        cur.execute('UPDATE clients SET phones = array_append(phones, %s) WHERE id = %s', (phone, client_id))
    conn.commit()

def change_client(conn, client_id, first_name=None, last_name=None, email=None, phones=None):
    set_clause = []
    values = []

    if first_name is not None:
        set_clause.append('first_name = %s')
        values.append(first_name)
    if last_name is not None:
        set_clause.append('last_name = %s')
        values.append(last_name)
    if email is not None:
        set_clause.append('email = %s')
        values.append(email)
    if phones is not None:
        set_clause.append('phones = %s')
        values.append(phones)

    set_str = ', '.join(set_clause)
    values.append(client_id)

    with conn.cursor() as cur:
        cur.execute('UPDATE clients SET ' + set_str + ' WHERE id = %s', values)
    conn.commit()

def delete_phone(conn, client_id, phone):
    with conn.cursor() as cur:
        cur.execute('UPDATE clients SET phones = array_remove(phones, %s) WHERE id = %s', (phone, client_id))
    conn.commit()

def delete_client(conn, client_id):
    with conn.cursor() as cur:
        cur.execute('DELETE FROM clients WHERE id = %s', (client_id,))
    conn.commit()

def find_client(conn, first_name='', last_name='', email='', phone=''):
    with conn.cursor() as cur:
        cur.execute('''
            SELECT * FROM clients WHERE
            (first_name = %s OR %s = '') AND 
            (last_name = %s OR %s = '') AND 
            (email = %s OR %s = '') AND 
            (%s = ANY(phones) OR %s = '')
        ''', (first_name, first_name, last_name, last_name, email, email, phone, phone))
        clients = cur.fetchall()
    return clients

if __name__ == "__main__":
    conn = psycopg2.connect(database="data_users", user="postgres", password="postgres")
    with conn:
        create_db(conn)
        add_client(conn, "Павел", "Дуров", "Man@example.com", ["723456789", "987654321"])
        add_phone(conn, 1, "756877772")
        change_client(conn, 1, first_name="Неприделах")
        delete_phone(conn, 1, "628454789")
        print(find_client(conn, first_name="Неприделах"))
