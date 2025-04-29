from http import HTTPStatus
from flask import Flask, jsonify, request
import db

app = Flask(__name__)


# проверка подключения
@app.route("/", methods=["GET"])
def ping():
    return jsonify({"message": "Server is start"}), HTTPStatus.OK


######
#POST#
######


# добавление товара
@app.route("/insert_goods", methods=["POST"])
def insert_goods():
    good = request.get_json()

    if good['name_goods'] is None or good['quantity'] is None or good['name_category'] is None or good['full_cost'] is None:
        return jsonify({"error": "info incorrect"}), HTTPStatus.BAD_REQUEST
    elif good['name_goods'] == '' or good['quantity'] == '' or good['name_category'] == '' or good[
        'full_cost'] == '':
        return jsonify({"error": "info incorrect"}), HTTPStatus.BAD_REQUEST

    db.insert_goods(good['name_goods'], good['quantity'], good['name_category'], good['full_cost'])

    return jsonify({"message": "good added"}), HTTPStatus.CREATED


# добавление операции покупки товара
@app.route("/insert_order_buy", methods=["POST"])
def insert_order_buy():
    buy_info = request.get_json()

    if buy_info['goods'] is None or buy_info['supplier'] is None or buy_info['price'] is None or buy_info[
        'quantity'] is None or buy_info['date'] is None:

        return jsonify({"error": "info incorrect"}), HTTPStatus.BAD_REQUEST
    elif buy_info['goods'] == '' or buy_info['supplier'] == '' or buy_info['price'] == '' or buy_info[
        'quantity'] == '' or buy_info['date'] == '':

        return jsonify({"error": "info incorrect"}), HTTPStatus.BAD_REQUEST

    db.insert_order_buy(buy_info['goods'], buy_info['supplier'], buy_info['price'], buy_info[
        'quantity'], buy_info['date'])

    return jsonify({"message": "buy info added in BD"}), HTTPStatus.CREATED


# добавление операции реализации товара
@app.route("/insert_order_sale", methods=["POST"])
def insert_order_sale():
    sale_info = request.get_json()

    if sale_info['goods'] is None or sale_info['customer'] is None or sale_info['price'] is None or sale_info[
        'quantity'] is None or sale_info['date'] is None:

        return jsonify({"error": "info incorrect"}), HTTPStatus.BAD_REQUEST
    elif sale_info['goods'] == '' or sale_info['customer'] == '' or sale_info['price'] == '' or sale_info[
        'quantity'] == '' or sale_info['date'] == '':

        return jsonify({"error": "info incorrect"}), HTTPStatus.BAD_REQUEST

    db.insert_order_sale(sale_info['goods'], sale_info['customer'], sale_info['price'], sale_info[
        'quantity'], sale_info['date'])

    return jsonify({"message": "sale info added in BD"}), HTTPStatus.CREATED


# добавление банковской операции
@app.route("/insert_bank_transaction", methods=["POST"])
def insert_bank_transaction():
    transaction = request.get_json()

    if transaction['transaction_amount'] is None or transaction['description'] is None or transaction['transaction_date'] is None:
        return jsonify({"error": "info incorrect"}), HTTPStatus.BAD_REQUEST
    elif transaction['transaction_amount'] == '' or transaction['description'] == '' or transaction['transaction_date'] == '':
        return jsonify({"error": "info incorrect"}), HTTPStatus.BAD_REQUEST

    db.insert_bank_transaction(transaction['transaction_amount'], transaction['description'], transaction['transaction_date'])

    return jsonify({"message": "added bank transaction"}), HTTPStatus.CREATED


# добавление категории товара
@app.route("/insert_categories_goods", methods=["POST"])
def insert_categories_goods():
    categories = request.get_json()
    if "name" not in categories:
        return jsonify({"error": "json incorrect, not key 'name"}), HTTPStatus.BAD_REQUEST
    if categories['name'] is None:
        return jsonify({"error": "info incorrect"}), HTTPStatus.BAD_REQUEST
    elif categories['name'] == '':
        return jsonify({"error": "info incorrect"}), HTTPStatus.BAD_REQUEST

    db.insert_categories(categories['name'])

    return jsonify({"message": "added categories in BD"}), HTTPStatus.CREATED


# добавление поставщика
@app.route("/insert_suppliers", methods=["POST"])
def insert_suppliers():
    suppliers = request.get_json()
    if "name" not in suppliers:
        return jsonify({"error": "json incorrect, not key 'name"}), HTTPStatus.BAD_REQUEST
    if suppliers['name'] is None:
        return jsonify({"error": "info incorrect"}), HTTPStatus.BAD_REQUEST
    elif suppliers['name'] == '':
        return jsonify({"error": "info incorrect"}), HTTPStatus.BAD_REQUEST

    db.insert_suppliers(suppliers['name'])

    return jsonify({"message": "added suppliers in BD"}), HTTPStatus.CREATED


# добавление клиента
@app.route("/insert_customers", methods=["POST"])
def insert_customers():
    customers = request.get_json()
    if "name" not in customers:
        return jsonify({"error": "json incorrect, not key 'name"}), HTTPStatus.BAD_REQUEST
    if customers['name'] is None:
        return jsonify({"error": "info incorrect"}), HTTPStatus.BAD_REQUEST
    elif customers['name'] == '':
        return jsonify({"error": "info incorrect"}), HTTPStatus.BAD_REQUEST

    db.insert_customer(customers['name'])

    return jsonify({"message": "added customers in BD"}), HTTPStatus.CREATED


#####
#GET#
#####


# получение банковский транзакций
@app.route("/transactions/<string:date1>/<string:date2>", methods=["GET"])
def get_transactions_by_period(date1, date2):

    # нужна лучшая валидация дат!!
    if date1 is not None and date2 is not None:
        try:
            transactions_info = db.get_transaction(date1, date2)
            transactions = list()

            for tran in transactions_info:
                transactions.append({
                    "id": tran[0],
                    "transaction_amount": tran[1],
                    "description": tran[2],
                    "transaction_date": tran[3]
                })
            return jsonify({"transactions": transactions}), HTTPStatus.OK
        except Exception as e:
            return jsonify({"message": f"Error - {e}"}), HTTPStatus.BAD_REQUEST
    else:
        return jsonify({"message": "incorrect date in API"}), HTTPStatus.BAD_REQUEST


# движение релаизации товара
@app.route("/order_sale/<string:date1>/<string:date2>", methods=["GET"])
def get_orders_sale(date1, date2):

    # нужна лучшая валидация дат!!
    if date1 is not None and date2 is not None:
        try:
            orders_sale = db.get_orders_sale(date1, date2)
            sale = list()

            for row in orders_sale:
                sale.append({
                    "id": row[0],
                    "good_name": row[1],
                    "price": row[2],
                    "quantity": row[3],
                    "date": row[4],
                    "customers_name": row[5]
                })
            return jsonify({"orders_sale": sale}), HTTPStatus.OK
        except Exception as e:
            return jsonify({"message": f"Error - {e}"}), HTTPStatus.BAD_REQUEST
    else:
        return jsonify({"message": "incorrect date in API"}), HTTPStatus.BAD_REQUEST


# движение закупки товара
@app.route("/order_buy/<string:date1>/<string:date2>", methods=["GET"])
def get_orders_buy(date1, date2):

    # нужна лучшая валидация дат!!
    if date1 is not None and date2 is not None:
        try:
            orders_buy = db.get_orders_buy(date1, date2)
            buy = list()

            for row in orders_buy:
                buy.append({
                    "id": row[0],
                    "good_name": row[1],
                    "price": row[2],
                    "quantity": row[3],
                    "date": row[4],
                    "supplier_name": row[5]
                })
            return jsonify({"orders_buy": buy}), HTTPStatus.OK
        except Exception as e:
            return jsonify({"message": f"Error - {e}"}), HTTPStatus.BAD_REQUEST
    else:
        return jsonify({"message": "incorrect date in API"}), HTTPStatus.BAD_REQUEST


# инфо по товарам
@app.route("/goods_info", methods=["GET"])
def get_goods():
    goods_info = db.get_goods()

    goods = list()

    for good in goods_info:
        goods.append({
            "id": good[0],
            "name": good[1],
            "quantity": good[2],
            "full_cost": good[3],
            "category_name": good[4]
        })

    return jsonify({"goods": goods}), HTTPStatus.OK


# инфо по товарам в разрезе категорий
@app.route("/category_goods_info", methods=["GET"])
def get_goods_category():
    goods_info = db.get_goods_by_categories()

    goods = list()

    for good in goods_info:
        goods.append({
            "category": good[0],
            "sum_quantity": good[1],
            "sum_full_cost": good[2]
        })

    return jsonify({"goods_by_category": goods}), HTTPStatus.OK


# поиск движения товара по лайк клиента
@app.route("/search_order_by_client/<string:client>", methods=["GET"])
def get_orders_sale_by_client(client):
    if client is not None and client != "":
        orders_by_client = db.get_orders_sale_by_client(client)

        obc = list()

        for row in orders_by_client:
            obc.append({
                "id": row[0],
                "good_name": row[1],
                "price": row[2],
                "quantity": row[3],
                "date": row[4],
                "client_name": row[5]
            })

        return jsonify({"orders_by_client": obc}), HTTPStatus.OK
    return jsonify({"message": "error"}), HTTPStatus.BAD_REQUEST


# поиск движения товара по лайк поставщика
@app.route("/search_order_by_suppliers/<string:suppliers>", methods=["GET"])
def get_orders_buy_suppliers(suppliers):
    if suppliers is not None and suppliers != "":
        orders_by_suppliers = db.get_orders_buy_suppliers(suppliers)

        obs = list()

        for row in orders_by_suppliers:
            obs.append({
                "id": row[0],
                "good_name": row[1],
                "price": row[2],
                "quantity": row[3],
                "date": row[4],
                "supplier_name": row[5]
            })

        return jsonify({"orders_by_suppliers": obs}), HTTPStatus.OK
    return jsonify({"message": "error"}), HTTPStatus.BAD_REQUEST

#=================#
#Запуск приложения#
#=================#


if __name__ == "__main__":
    try:
        db.init_db()
        print("BD init!")
    except Exception as e:
        print(f"Error init DB - {e}")

    app.run(port=5005, debug=True)
