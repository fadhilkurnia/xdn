import socket
import traceback

from flask import Flask, request, jsonify
from werkzeug.exceptions import HTTPException

from tpcc.init_db import init_db
from tpcc.transactions import *

app = Flask(__name__)


# Routes
#
# POST /transaction/new_order      => POST /orders
# POST /transaction/payment        => POST /payment
# POST /transaction/delivery       => GET /warehouses/:warehouseId/deliveries
# POST /transaction/order_status   => GET /customers/:customerId/orders
# POST /transaction/stock_level    => GET /warehouses/:warehouseId/stock


def transaction_route(transaction_fn, data):
    result = transaction_fn(**data)
    return jsonify(result=result)


@app.route("/health")
def health():
    return jsonify(health=True, host=socket.gethostname())


@app.route("/orders", methods=["POST"])
def new_order_route():
    data = request.json
    return transaction_route(new_order_tran, data)


@app.route("/payment", methods=["POST"])
def payment():
    data = request.json
    return transaction_route(payment_tran, data)


@app.route("/customers/<int:customer_id>/orders", methods=["GET"])
def order_status_route(customer_id: int):
    return transaction_route(order_status_tran, {'c_id': customer_id})


@app.route("/warehouses/<int:warehouse_id>/deliveries", methods=["POST"])
def delivery_route(warehouse_id: int):
    return transaction_route(delivery_tran, {'w_id': warehouse_id})


@app.route("/warehouses/<int:warehouse_id>/stock", methods=["GET"])
def stock_level_route(warehouse_id: int):
    return transaction_route(stock_level_tran, {'w_id': warehouse_id})


@app.route("/init_db", methods=["POST"])
def init_db_route():
    try:
        warehouses = request.json['warehouses']
    except:
        warehouses = WAREHOUSES

    init_db(warehouses)
    return jsonify(result=True)


@app.errorhandler(Exception)
def handle_exception(exception):
    status_code = 500
    if isinstance(exception, TypeError):
        status_code = 400
    elif isinstance(exception, HTTPException):
        status_code = exception.code

    return jsonify(error=type(exception).__name__, message=str(exception),
                   traceback=traceback.format_exc()), status_code
