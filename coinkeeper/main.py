from http import HTTPStatus
from flask import Flask, jsonify, request
from coinkeeper import db

app = Flask(__name__)

@app.route("/", methods=["GET"])
def route():
    return jsonify({"message": "Server is start!"}), HTTPStatus.OK


@app.route("/add-account", methods=["POST"])
def add_account():
    name_acc = request.get_json()

    db.create_account(name_acc['name_account'])

    return jsonify({"message": "account created"}), HTTPStatus.CREATED



@app.route("/income", methods=["POST"])
def add_income():
    income = request.get_json()

    db.add_transaction_income(
        income['description'],
        income['id_account'],
        income['sum_transaction'],
        income['date_transaction']
    )

    return jsonify({"message": "transaction added to account"}), HTTPStatus.CREATED


@app.route("/expense", methods=["POST"])
def add_expense():
    income = request.get_json()

    db.add_transaction_expense(
        income['description'],
        income['id_account'],
        income['sum_transaction'],
        income['date_transaction']
    )

    return jsonify({"message": "transaction added to account"}), HTTPStatus.CREATED


@app.route("/get-balance-acc/<int:id_acc>", methods=["GET"])
def get_balance_acc(id_acc):

    balance_info = db.get_balance_acc(id_acc)

    if balance_info is None:
        return jsonify({"message": "response is None"})

    acc_info = {
        "name_account": balance_info[0],
        "balance": balance_info[1]
    }

    return jsonify(acc_info), HTTPStatus.OK


@app.route("/get-income", methods=["GET"])
def get_income():
    transactions_info = db.get_income()

    transactions = list()

    for info in transactions_info:
        transactions.append(
            {
                "description": info[0],
                "sum_transaction": info[1],
                "date_transaction": info[2],
                "name_account": info[3]
            }
        )

    return jsonify({"transactions": transactions})


@app.route("/get-expense", methods=["GET"])
def get_expense():
    transactions_info = db.get_expense()

    transactions = list()

    for info in transactions_info:
        transactions.append(
            {
                "description": info[0],
                "sum_transaction": info[1],
                "date_transaction": info[2],
                "name_account": info[3]
            }
        )

    return jsonify({"transactions": transactions})


if __name__ == "__main__":
    try:
        db.init_db()
    except Exception as e:
        print(f"Ошибка соединения с БД: {e}")

    app.run(port=5002, debug=True)
