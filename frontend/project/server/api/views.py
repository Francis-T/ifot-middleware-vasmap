import redis
import os
import time
import pandas as pd
import multiprocessing
import json

from rq import Queue, Connection
from flask import Flask, Blueprint, request, jsonify, current_app

from ...common.defs import *
from ...common import pandas_tools, general_tools, query_tools, redis_tools
from ..main import metas
from ..services import generic, vas

api_blueprint = Blueprint('api', __name__,)

def enqueue_task(queue, unique_id, seq_id, redis_df_key):
  r = redis.StrictRedis(host='redis', port=6380)
  with Connection(r):
    q = Queue('default')
    task = q.enqueue('tasks.classify_iris', unique_id, seq_id, redis_df_key)
  queue.put(task.get_id())
  return

@api_blueprint.route('/receive', methods=['POST'])
def receive():
  data = request.get_json(force=True)
  print(data['key'])
  response = {'response': 'hello from the api side'}
  return jsonify(response)

#Should work as long as you have access to this url, no flask needed
@api_blueprint.route('/iris_dist_process', methods=['POST'])
def iris_dist_process():
  data = request.get_json(force=True)
  filename = data['filename']
  nodes = data["nodes"]
  r = redis.StrictRedis(host='redis', port=6380, decode_responses=True)
  print(filename)
  try:
    if filename and general_tools.allowed_file(filename):
      unique_id = query_tools.initialize_query(nodes,
                                               count_suffix=TASK_COUNT,
                                               done_count_suffix=DONE_TASK_COUNT)

      with open(os.path.join(current_app.instance_path, 'htmlfi', filename)) as f:
        df = pd.read_csv(f, header=None)
        print(df.shape)
        df_arr = pandas_tools.df_split(df, nodes)
        print(len(df_arr))

        response = {}
        response['query_ID'] = unique_id
        response['query_received'] = query_tools.get_current_time()

        task_ids = []
        processes = []
        mpq = multiprocessing.Queue()

        seq_id = 0

        for df in df_arr:
          df_key = redis_tools.store_dataframe_with_key(r, unique_id + '_' + str(seq_id), df)
          print(df_key)

          p = multiprocessing.Process(target=enqueue_task, 
                                      args=(mpq, unique_id, seq_id, df_key))
          processes.append(p)
          p.start()

          seq_id += 1

        for p in processes:
          task = mpq.get()
          task_ids.append(task)

        for p in processes:
          p.join()
        toc = time.perf_counter()
      response_object = {
        'status': 'success',
        'unique_ID': unique_id,
        # 'params': {
        #   'start_time'  : start_time,
        #   'end_time'    : end_time,
        #   'split_count' : split_count
        # },
        'data': {
          'task_id': task_ids
        },
        # "benchmarks" : {
        #   "exec_time" : str(toc - tic),
        # }
      }
      response['response_object'] = response_object
      print(response)
      return jsonify(response)
  except IOError:
    pass
  return "Unable to read file"

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

@api_blueprint.route('/vas/get_average_speeds', methods=['GET'])
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

##
##  IFoT Middleware Service Management Utility Functions
##
def call_service(service_func, params, task_graph):
  # Execute the request
  api_resp = service_func(params, task_graph)
  return jsonify(api_resp), 202

def forward_request(service_func, request):
  # Execute the request
  api_resp = service_func(request)
  return jsonify(api_resp), 202



