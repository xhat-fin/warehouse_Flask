import psycopg2


DB_CONFIG = {
    "dbname": "coinkeeper",
    "user": "postgres",
    "password": "password",
    "host": "localhost",
    "port": "5432"
}

def db_connect():
    return psycopg2.connect(**DB_CONFIG)


def init_db():
    with db_connect() as conn, conn.cursor() as cur:

        cur.execute("""
            CREATE TABLE IF NOT EXISTS accounts 
                (
                    id SERIAL PRIMARY KEY,
                    name_account VARCHAR(50),
                    balance DECIMAL(10, 2)
                );
            """)


        cur.execute("""
            CREATE TABLE IF NOT EXISTS income
                (
                    id SERIAL PRIMARY KEY,
                    description VARCHAR,
                    id_account INT REFERENCES accounts(id),
                    sum_transaction DECIMAL(15, 2), 
                    date_transaction DATE
                )
            """)


        cur.execute("""
            CREATE TABLE IF NOT EXISTS expense
                (
                    id SERIAL PRIMARY KEY,
                    description VARCHAR,
                    id_account INT REFERENCES accounts(id),
                    sum_transaction DECIMAL(15, 2), 
                    date_transaction DATE
                );
            """)

    conn.commit()

def add_transaction_income(description, id_account, sum_transaction, date_transaction):
    with db_connect() as conn:
        with conn.cursor() as cur:

            cur.execute("""
                INSERT INTO income 
                (
                    description, 
                    id_account, 
                    sum_transaction, 
                    date_transaction
                ) 
                VALUES (%s, %s, %s, %s) 
            """, (description, id_account, sum_transaction, date_transaction,))

            cur.execute("""
                UPDATE accounts SET balance = balance + %s WHERE id = %s
            """, (sum_transaction, id_account))

    conn.commit()


def add_transaction_expense(description, id_account, sum_transaction, date_transaction):
    with db_connect() as conn:
        with conn.cursor() as cur:

            cur.execute("""
                INSERT INTO expense 
                (
                    description, 
                    id_account, 
                    sum_transaction, 
                    date_transaction
                ) 
                VALUES (%s, %s, %s, %s) 
            """, (description, id_account, sum_transaction, date_transaction,))

            cur.execute("""
                UPDATE accounts SET balance = balance - %s WHERE id = %s
            """, (sum_transaction, id_account))

    conn.commit()


def create_account(name_account):
    with db_connect() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO accounts (name_account, balance) VALUES (%s, %s)
            """, (name_account, 0))
    conn.commit()


def get_balance_acc(id_acc):
    with db_connect() as conn:
        with conn.cursor() as cur:
            cur.execute("""
            SELECT name_account, balance FROM accounts WHERE id = %s
            """, (id_acc,))

            balance = cur.fetchone()
    return balance

def get_income():
    with db_connect() as conn:
        with conn.cursor() as cur:
            cur.execute("""
            select income.description, sum_transaction, date_transaction, name_account from income left join accounts ON income.id_account = accounts.id 
            """)
            transactions = cur.fetchall()

    return transactions


def get_expense():
    with db_connect() as conn:
        with conn.cursor() as cur:
            cur.execute("""
            select expense.description, sum_transaction, date_transaction, name_account from expense left join accounts ON expense.id_account = accounts.id
            """)
            transactions = cur.fetchall()

    return transactions