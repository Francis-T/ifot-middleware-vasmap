import redis
import os
import time
import pandas as pd
import multiprocessing
import json

from rq import Queue, Connection
from flask import request, jsonify, current_app

from ...common.defs import *
from ...common import pandas_tools, general_tools, query_tools, redis_tools

def dist_process(request):
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

