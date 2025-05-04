from sqlalchemy import create_engine, ForeignKey, func, between
from sqlalchemy.orm import DeclarativeBase, sessionmaker, Session, relationship, foreign
from sqlalchemy import Column, Integer, String, Numeric, Date
from sqlalchemy.sql.operators import ilike_op

DB_CONFIG = {
    "dbname": "warehouse",
    "user": "postgres",
    "password": "Totem151012",
    "host": "localhost",
    "port": "5432"
}


engine = create_engine(url = f"postgresql://{DB_CONFIG['user']}:{DB_CONFIG['password']}@{DB_CONFIG['host']}/{DB_CONFIG['dbname']}")


# определяем модели таблиц
class Base(DeclarativeBase): pass



class CategoriesGoods(Base):
    __tablename__ = "categories_goods"

    id = Column(Integer, primary_key=True, index=True)
    name = Column(String)
    goods = relationship("Goods", back_populates="categories")


class Customers(Base):
    __tablename__ = "customers"

    id = Column(Integer, primary_key=True, index=True)
    name = Column(String)

    sale = relationship("OrdersSale", back_populates="customers")


class Suppliers(Base):
    __tablename__ = "suppliers"

    id = Column(Integer, primary_key=True, index=True)
    name = Column(String)

    order_buy = relationship("OrdersBuy", back_populates="supplier")


class Goods(Base):
    __tablename__ = "goods"

    id = Column(Integer, primary_key=True, index=True)
    name = Column(String)
    quantity = Column(Integer)
    category_id = Column(Integer, ForeignKey("categories_goods.id")) #key
    full_cost = Column(Numeric(10, 2))


    categories = relationship("CategoriesGoods", back_populates="goods")
    orders_buy = relationship("OrdersBuy", back_populates="goods")
    orders_sale = relationship("OrdersSale", back_populates="goods")


class OrdersBuy(Base):
    __tablename__ = "orders_buy"

    id = Column(Integer, primary_key=True, index=True)
    goods_id = Column(Integer, ForeignKey("goods.id")) #key
    supplier_id = Column(Integer, ForeignKey("suppliers.id")) #key
    price = Column(Numeric(10, 2))
    quantity = Column(Integer)
    date = Column(Date)

    goods = relationship("Goods", back_populates="orders_buy")
    supplier = relationship("Suppliers", back_populates="order_buy")


class OrdersSale(Base):
    __tablename__ = "orders_sale"

    id = Column(Integer, primary_key=True, index = True)
    goods_id = Column(Integer, ForeignKey("goods.id")) #key
    customers_id = Column(Integer, ForeignKey("customers.id")) #key
    price = Column(Numeric(10, 2))
    quantity = Column(Integer)
    date = Column(Date)

    goods = relationship("Goods", back_populates="orders_sale")
    customers = relationship("Customers", back_populates="sale")

class BankAccount(Base):
    __tablename__ = "bank_account"

    id = Column(Integer, primary_key=True, index=True)
    transaction_amount = Column(Numeric(15,2))
    description = Column(String)
    transaction_date = Column(Date)


def init_db():
    Base.metadata.create_all(bind=engine)
    print("Соединение успешно")


#====================================#


def insert_categories(categories):
    with Session(autoflush=False, bind=engine) as db:
        categories_goods = CategoriesGoods(
            name = categories
        )
        db.add(categories_goods)
        db.commit()


def insert_goods(name_goods, quantity, category_id, full_cost):
    with Session(autoflush=False, bind=engine) as db:
        good = Goods(name = name_goods,
                     quantity = quantity,
                     category_id = category_id,
                     full_cost = full_cost)
        db.add(good)
        db.commit()


def insert_bank_transaction(transaction_amount, description, transaction_date):
    with Session(autoflush=False, bind=engine) as db:
        transaction = BankAccount(
            transaction_amount = transaction_amount,
            description = description,
            transaction_date = transaction_date
        )
        db.add(transaction)
        db.commit()


def insert_customer(customer):
    with Session(autoflush=False, bind=engine) as db:
        new_customer = Customers(name=customer)
        db.add(new_customer)
        db.commit()


def insert_suppliers(supplier):
    with Session(autoflush=False, bind=engine) as db:
        new_suppliers = Suppliers(name=supplier)
        db.add(new_suppliers)
        db.commit()


def insert_order_sale(goods_id, customer_id, price, quantity, date):
    try:

        with Session(autoflush=False, bind=engine) as db:
            if not db.query(Goods).get(goods_id):
                return ValueError("Goods not found")

            if not db.query(Suppliers).get(customer_id):
                return ValueError("Customer not found")

            order_sale = OrdersSale(
                goods_id = goods_id,
                customers_id = customer_id,
                price = price,
                quantity = quantity,
                date = date
            )
            db.add(order_sale)
            db.commit()

    except Exception as e:
        return {"message": f"Error {e}"}

def insert_order_buy(goods_id, supplier_id, price, quantity, date):
    try:
        with Session(autoflush=False, bind=engine) as db:
            if not db.query(Goods).get(goods_id):
                raise ValueError("Goods not found")

            if not db.query(Suppliers).get(supplier_id):
                raise ValueError("Supplier not found")

            orders_buy = OrdersBuy(
                goods_id = goods_id,
                supplier_id = supplier_id,
                price = price,
                quantity = quantity,
                date = date
            )
            db.add(orders_buy)
            db.commit()
    except Exception as e:
        return {"message": f"Error {e}"}



def get_transaction(date_1, date_2):
    with Session(autoflush=False, bind=engine) as db:
        transaction = db.query(
            BankAccount.id,
            BankAccount.transaction_amount,
            BankAccount.description,
            BankAccount.transaction_date
            ).filter(between(BankAccount.transaction_date, date_1, date_2)).all()
        return transaction


def get_orders_sale(date1, date2):
    with Session(autoflush=False, bind=engine) as db:
        order_sale = db.query(
            OrdersSale.id,
            Goods.name.label("Товар"),
            OrdersSale.price.label("Цена партии"),
            OrdersSale.quantity.label("Количество в партии"),
            OrdersSale.date.label("Дата ТН"),
            Customers.name.label("Поставщик")
        ).join(Goods, OrdersSale.goods_id == Goods.id
        ).join(Customers, OrdersSale.customers_id == Customers.id
        ).filter(between(OrdersSale.date, date1, date2)).all()

        return order_sale


def get_orders_buy(date_1, date_2):
    with Session(autoflush=False, bind=engine) as db:
        orders_buy = db.query(
            OrdersBuy.id,
            Goods.name.label("Товар"),
            OrdersBuy.price.label("Цена партии"),
            OrdersBuy.quantity.label("Количество в партии"),
            OrdersBuy.date.label("Дата ТН"),
            Suppliers.name.label("Поставщик")
        ).join(Goods, OrdersBuy.goods_id == Goods.id
        ).join(Suppliers, OrdersBuy.supplier_id == Suppliers.id
        ).filter(between(OrdersBuy.date, date_1, date_2)).all()

        return orders_buy


def get_goods():
    with Session(autoflush=False, bind=engine) as db:
        goods = db.query(
            Goods.id,
            Goods.name.label("Товар"),
            Goods.quantity.label("Количество"),
            Goods.full_cost.label("Полная стоимость"),
            CategoriesGoods.name.label("Категория")
            ).join(CategoriesGoods, Goods.category_id == CategoriesGoods.id, isouter = True).all()
        return goods


def get_goods_by_categories():
    with Session(autoflush=False, bind=engine) as db:
        goods_by_categories = db.query(
            CategoriesGoods.name.label("Категория"),
            func.sum(Goods.quantity).label("Количество"),
            func.sum(Goods.full_cost).label("Общая стоимость")
            ).join(Goods, CategoriesGoods.id == Goods.category_id, isouter = True).group_by(CategoriesGoods.name).all()
        return goods_by_categories


def get_orders_buy_suppliers(supplier):
    with Session(autoflush=False, bind=engine) as db:
        orders_buy = db.query(
            OrdersBuy.id,
            Goods.name.label("Товар"),
            OrdersBuy.price.label("Цена партии"),
            OrdersBuy.quantity.label("Количество в партии"),
            OrdersBuy.date.label("Дата ТН"),
            Suppliers.name.label("Поставщик")
        ).join(Goods, OrdersBuy.goods_id == Goods.id
        ).join(Suppliers, OrdersBuy.supplier_id == Suppliers.id
        ).filter(Suppliers.name.ilike(f"%{supplier}%")).all()

        return orders_buy


def insert_orders_sale(goods_id, customers_id, price, quantity, date):
    with Session(autoflush=False, bind=engine) as db:
        order_sale = OrdersSale(
            goods_id = goods_id,
            customers_id = customers_id,
            price = price,
            quantity = quantity,
            date = date
        )
        db.add(order_sale)
        db.commit()


def get_orders_sale_by_client(client):
    with Session(autoflush=False, bind=engine) as db:
        orders_sale = db.query(
            OrdersSale.id,
            Goods.name.label("Товар"),
            OrdersSale.price.label("Цена партии"),
            OrdersSale.quantity.label("Количество в партии"),
            OrdersSale.date.label("Дата ТН"),
            Customers.name.label("Поставщик")
        ).join(Goods, OrdersSale.goods_id == Goods.id
        ).join(Customers, OrdersSale.customers_id == Customers.id
        ).filter(Customers.name.ilike(f"%{client}%")).all()

        return orders_sale



def get_orders_buy_suppliers(supplier):
    with Session(autoflush=False, bind=engine) as db:
        orders_buy = db.query(
            OrdersBuy.id,
            Goods.name.label("Товар"),
            OrdersBuy.price.label("Цена партии"),
            OrdersBuy.quantity.label("Количество в партии"),
            OrdersBuy.date.label("Дата ТН"),
            Suppliers.name.label("Поставщик")
        ).join(Goods, OrdersBuy.goods_id == Goods.id
        ).join(Suppliers, OrdersBuy.supplier_id == Suppliers.id
        ).filter(Suppliers.name.ilike(f"%{supplier}%")).all()

        return orders_buy
