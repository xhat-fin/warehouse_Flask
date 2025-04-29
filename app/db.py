import psycopg2

DB_CONFING = {
    "dbname": "warehouse",
    "user": "postgres",
    "password": "Totem151012",
    "host": "localhost",
    "port": "5432"
}


def connect_db():
    return psycopg2.connect(**DB_CONFING)


def init_db():
    with connect_db() as conn, conn.cursor() as cur:
        cur.execute("""
            CREATE TABLE IF NOT EXISTS categories_goods(
            id SERIAL PRIMARY KEY,
            name VARCHAR(30)
            );
            """)

        cur.execute("""
            CREATE TABLE IF NOT EXISTS customers(
            id SERIAL PRIMARY KEY,
            name VARCHAR(30)
            );
            """)

        cur.execute("""
            CREATE TABLE IF NOT EXISTS suppliers(
            id SERIAL PRIMARY KEY,
            name VARCHAR(30)
            );
            """)

        cur.execute("""
            CREATE TABLE IF NOT EXISTS goods(
            id SERIAL PRIMARY KEY,
            name VARCHAR(30),
            quantity INT CHECK (quantity>=0),
            category_id INT REFERENCES categories_goods (id),
            full_cost DECIMAL(15, 2)
            );
            """)

        cur.execute("""
            CREATE TABLE IF NOT EXISTS orders_buy(
            id SERIAL PRIMARY KEY,
            goods_id INT REFERENCES goods(id),
            supplier_id INT REFERENCES suppliers(id),
            price DECIMAL(15, 2),
            quantity INT,
            date DATE
            );
            """)

        cur.execute("""
            CREATE TABLE IF NOT EXISTS orders_sale(
            id SERIAL PRIMARY KEY,
            goods_id INT REFERENCES goods(id),
            customers_id INT REFERENCES customers(id),
            price DECIMAL(15, 2),
            quantity INT,
            date DATE
            );
            """)

        cur.execute("""
            CREATE TABLE IF NOT EXISTS bank_account(
            id SERIAL PRIMARY KEY,
            transaction_amount DECIMAL(15,2),
            description VARCHAR(500),
            transaction_date DATE
            );
            """)

    conn.close()


#====================================#


def insert_categories(categories):
    with connect_db() as conn, conn.cursor() as cur:
        cur.execute("""INSERT INTO categories_goods (name) VALUES (%s)""", (categories,))
    conn.close()


def insert_goods(name_goods, quantity, name_category, full_cost):
    with connect_db() as conn, conn.cursor() as cur:

        cur.execute("""SELECT id FROM categories_goods WHERE name ILIKE %s
        """,(f"%{name_category.strip()}%",))

        id_category = cur.fetchone()


        if id_category is None:
            insert_categories(name_category)


        cur.execute("""SELECT id FROM categories_goods WHERE name ILIKE %s
        """,(f"%{name_category.strip()}%",))

        id_category = cur.fetchone()


        cur.execute("""
        INSERT INTO goods (name, quantity, category_id, full_cost) VALUES (%s, %s, %s, %s)
        """, (name_goods, quantity, id_category, full_cost))
    conn.close()


def insert_bank_transaction(transaction_amount, description, transaction_date):
    with connect_db() as conn, conn.cursor() as cur:
        cur.execute("""INSERT INTO bank_account(transaction_amount, description, transaction_date) 
            VALUES (%s, %s, %s)""",
                    (transaction_amount, description, transaction_date))
    conn.close()


def insert_customer(customer):
    with connect_db() as conn, conn.cursor() as cur:
        cur.execute("""INSERT INTO customers (name) VALUES (%s)""", (customer,))
    conn.close()


def insert_suppliers(supplier):
    with connect_db() as conn, conn.cursor() as cur:
        cur.execute("""INSERT INTO suppliers (name) VALUES (%s)""", (supplier,))
    conn.close()


def insert_order_sale(goods, customer, price, quantity, date):
    with connect_db() as conn, conn.cursor() as cur:
        cur.execute("""SELECT id FROM goods WHERE name ILIKE %s
        """,(f"%{goods.strip()}%",))

        goods_id = cur.fetchone()

        cur.execute("""SELECT id FROM customers WHERE name ILIKE %s
        """,(f"%{customer.strip()}%",))

        customer_id = cur.fetchone()

        cur.execute("""
        INSERT INTO orders_sale(goods_id, customers_id, price, quantity, date)
        VALUES (%s, %s, %s, %s, %s)
        """, (goods_id, customer_id, price, quantity, date))
    conn.close()


def insert_order_buy(goods, supplier, price, quantity, date):
    with connect_db() as conn, conn.cursor() as cur:
        cur.execute("""SELECT id FROM goods WHERE name ILIKE %s
        """,(f"%{goods.strip()}%",))

        goods_id = cur.fetchone()

        cur.execute("""SELECT id FROM suppliers WHERE name ILIKE %s
        """,(f"%{supplier.strip()}%",))

        supplier_id = cur.fetchone()

        cur.execute("""
        INSERT INTO orders_buy(goods_id, supplier_id, price, quantity, date)
        VALUES (%s, %s, %s, %s, %s)
        """, (goods_id, supplier_id, price, quantity, date))
    conn.close()


def get_transaction(date_1, date_2):
    with connect_db() as conn, conn.cursor() as cur:
        cur.execute("""SELECT id, transaction_amount, description, transaction_date FROM bank_account 
        WHERE transaction_date BETWEEN %s and %s""",
                    (date_1, date_2))
        transaction = cur.fetchall()
    conn.close()
    return transaction


def get_orders_sale(date1, date2):
    with connect_db() as conn, conn.cursor() as cur:
        cur.execute("""select o.id, g.name, o.price, o.quantity, o.date, c.name 
        from orders_sale o 
        LEFT JOIN customers c on o.customers_id = c.id 
        LEFT JOIN goods g on o.goods_id = g.id 
        where o.date between %s and %s""", (date1, date2))
        orders_sale = cur.fetchall()
    conn.close()
    return orders_sale


def get_orders_buy(date_1, date_2):
    with connect_db() as conn, conn.cursor() as cur:
        cur.execute("""select o.id, g.name as "Товар", o.price as "Цена партии", o.quantity as "Кол-во в партии", o."date" as "Дата ТН", s.name as "Поставщик"
        FROM orders_buy o join goods g on o.goods_id = g.id join suppliers s on o.supplier_id = s.id
        WHERE o.\"date\" BETWEEN %s and %s""", (date_1, date_2))
        orders_buy = cur.fetchall()
    conn.close()
    return orders_buy


def get_goods():
    with connect_db() as conn, conn.cursor() as cur:
        cur.execute("""
        select g.id, g.name as "Товар", g.quantity as "Количество", g.full_cost as "Общая стоимость", cg.name as "Категория" 
        from goods g left join categories_goods cg on g.category_id = cg.id
        """)
        goods = cur.fetchall()
    conn.close()
    return goods


def get_goods_by_categories():
    with connect_db() as conn, conn.cursor() as cur:
        cur.execute("""
        select cg.name as "Категория", SUM(g.quantity) as "Количество", SUM(g.full_cost) as "Общая стоимость" 
        from goods g left join categories_goods cg on g.category_id = cg.id group by cg.name
        """)
        goods_categories = cur.fetchall()
    conn.close()
    return goods_categories


def get_orders_buy_suppliers(supplier):
    with connect_db() as conn, conn.cursor() as cur:
        cur.execute("""select o.id, g.name as "Товар", o.price as "Цена партии", o.quantity as "Кол-во в партии", o."date" as "Дата ТН", s.name as "Поставщик"
        FROM orders_buy o join goods g on o.goods_id = g.id join suppliers s on o.supplier_id = s.id
        WHERE s.name ILIKE %s""", (f'%{supplier}%',))
        orders_buy = cur.fetchall()
    conn.close()
    return orders_buy


def get_orders_sale_by_client(client):
    with connect_db() as conn, conn.cursor() as cur:
        cur.execute("""select o.id, g.name, o.price, o.quantity, o.date, c.name 
        from orders_sale o 
        LEFT JOIN customers c on o.customers_id = c.id 
        left join goods g on o.goods_id = g.id where c.name ILIKE %s""", (f'%{client}%',))
        orders_sale = cur.fetchall()
    conn.close()
    return orders_sale
