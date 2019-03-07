import json

from flask import Flask, Blueprint, request, jsonify, current_app
from ..services import generic, vas, iris

from ..api import views

api_blueprint = Blueprint('api/vas', __name__,)

###
##  SEC0001: Vehicle Average Speed service API Functions
###
@api_blueprint.route('/get_average_speeds', methods=['POST'])
def get_average_speeds():
  return views.forward_request(vas.get_average_speeds, request)

@api_blueprint.route('/request_data_counts', methods=['GET'])
def request_data_counts():
  return jsonify(vas.get_data_counts())

@api_blueprint.route('/request_rsu_list', methods=['GET'])
def request_rsu_list():
  return jsonify(vas.get_rsu_list())

@api_blueprint.route('/get_last_task_graph', methods=['GET'])
def request_last_task_graph():
    task_graph = None
    with open('vas_task_graph.json', 'r') as task_graph_file:
        task_graph = json.load(task_graph_file)

    return jsonify(task_graph), 200

