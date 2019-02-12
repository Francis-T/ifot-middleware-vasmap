import redis
import os
import time
import pandas as pd
import multiprocessing
import json

from rq import Queue, Connection
from flask import Flask, Blueprint, request, jsonify, current_app

from ...common.defs import *
from ...common import metas
from ..services import generic, vas, iris

api_blueprint = Blueprint('api', __name__,)

###
##  SEC0001: Iris Classifier test service API Functions
###
@api_blueprint.route('/receive', methods=['POST'])
def receive():
  data = request.get_json(force=True)
  print(data['key'])
  response = {'response': 'hello from the api side'}
  return jsonify(response)

#Should work as long as you have access to this url, no flask needed
@api_blueprint.route('/iris_dist_process', methods=['POST'])
def iris_dist_process():
  return forward_request(iris.dist_process, request)

###
##  SEC0002: Vehicle Average Speed service API Functions
###
@api_blueprint.route('/vas/get_average_speeds', methods=['POST'])
def get_average_speeds():
  return forward_request(vas.get_average_speeds, request)

@api_blueprint.route('/vas/request_rsu_list', methods=['GET'])
def request_rsu_list():
  return jsonify(vas.get_rsu_list())

@api_blueprint.route('/vas/get_last_task_graph', methods=['GET'])
def request_last_task_graph():
    task_graph = None
    with open('vas_task_graph.json', 'r') as task_graph_file:
        task_graph = json.load(task_graph_file)

    #return send_from_directory('.', 'vas_task_graph.json')
    return jsonify(task_graph), 200

@api_blueprint.route('/run_service', methods=['GET'])
def run_service():
    req         = request.get_json(force=True)
    params      = req['params']
    task_graph  = req['task_graph']
    return call_service(generic.run_task_graph, params, task_graph)

###
##  SEC0003: Utility API Functions
###
@api_blueprint.route('/get_exec_times', methods=['GET', 'POST'])
def get_exec_times():
  data = {}
  # Get the execution timing info
  data['exec_time_logs'] = metas.get_all_exec_time_logs()
  return jsonify(data)

@api_blueprint.route('/get_exec_time/<unique_id>', methods=['GET', 'POST'])
def get_exec_time(unique_id):
  data = {}
  data['exec_time_logs'] = { unique_id : metas.get_exec_time_log(unique_id) }
  return jsonify(data)

###
##  SEC0004: IFoT Middleware Service Management Utility Functions
###
def call_service(service_func, params, task_graph):
  # Execute the request
  api_resp = service_func(params, task_graph)
  return jsonify(api_resp), 202

def forward_request(service_func, request):
  # Execute the request
  api_resp = service_func(request)
  return jsonify(api_resp), 202

def enqueue_task(queue, unique_id, seq_id, redis_df_key):
  r = redis.StrictRedis(host='redis', port=6380)
  with Connection(r):
    q = Queue('default')
    task = q.enqueue('tasks.classify_iris', unique_id, seq_id, redis_df_key)
  queue.put(task.get_id())
  return


