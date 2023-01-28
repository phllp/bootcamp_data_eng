from flask import Flask
import json

app = Flask(__name__)

@app.route('/titanic-data')
def titanic_data():
  with open('./data/train.csv', 'r') as file:
    return json.dumps(file.read())




  