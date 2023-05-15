from flask import Flask, request, render_template
from db import get_all_data, get_data_byID, edit_data, drop_data, create_data

app = Flask(__name__)

#Main page
@app.route('/')
def hello():
    return render_template('index.html')

#Endpoint: Get all the data
@app.route('/stackquestions', methods=['GET'])
def get_data():
    data = get_all_data()
    data = data.to_json(force_ascii=False).encode('utf-8')
    return data

#Endpoint: Get data by its id
@app.route('/stackquestions/<int:id>', methods=['GET'])
def get_data_by_id(id):
    data = get_data_byID(id)
    data = data.to_json(force_ascii=False).encode('utf-8')
    return data

#Endpoint: Modify/edit data by its id
@app.route('/stackquestions/<int:id>', methods=['PUT'])
def modify_data(id):
    data = request.get_json()
    edit_data(new_data=data, id=id)
    return data

#Endpoint: Delete data by its id
@app.route('/stackquestions/drop/<int:id>', methods=['DELETE'])
def delete_data(id):
    data = get_data_by_id(id)
    drop_data(id)
    return data

#Endpoint: Create data in the DataBase
@app.route('/stackquestions/create', methods=['POST'])
def make_data():
    data = request.get_json()
    create_data(jsonfile=data)
    return data


app.run(debug=True)

