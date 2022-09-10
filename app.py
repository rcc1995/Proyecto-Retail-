from flask import Flask, request, jsonify
from module.database import Database

app = Flask(__name__)
db = Database()

@app.route('/')
def index():
    return "hello world"

@app.route("/customer", methods = ['GET'])
def customer():
    if request.method == 'GET' :
        try:
            result = db.readCustomer(request.json["id"])
        except Exception as e:
            return e
    return jsonify(result)

@app.route("/subtotalByOneDepartment", methods = ['GET'])
def subtotalByOneDepartment():
    if request.method == 'GET' :
        try:
            result = db.readSubtotalByOneDepartment(request.json["id"])
        except Exception as e:
            return e
    return jsonify(result)

@app.route("/quantityByCategory", methods = ['GET'])
def quantityByCategory():
    if request.method == 'GET' :
        try:
            result = db.readQuantityByCategory()
        except Exception as e:
            return e
    return jsonify(result)

@app.route("/topTenCustomers", methods = ['GET'])
def topTenCustomers():
    if request.method == 'GET' :
        try:
            result = db.readTopTenCustomers()
        except Exception as e:
            return e
    return jsonify(result)

@app.route("/topTenProducts", methods = ['GET'])
def topTenProducts():
    if request.method == 'GET' :
        try:
            result = db.readTopTenProducts()
        except Exception as e:
            return e
    return jsonify(result)

@app.route("/subtotalByDepartment", methods = ['GET'])
def subtotalByDepartment():
    if request.method == 'GET' :
        try:
            result = db.readSubtotalByDepartment()
        except Exception as e:
            return e
    return jsonify(result)


if __name__=="main":
    app.debug = True
    app.run()