from flask import current_app

import redis
from rq import Queue, Connection

import time
import json
import multiprocessing

from ...common.defs import *
from ...common import query_tools, redis_tools

def enqueue_task(queue, task_graph, ref_id, params):
  with Connection(redis.from_url(current_app.config['REDIS_URL'])):
    q = Queue(task_graph[ref_id]['node_id'])

    task = q.enqueue(task_graph[ref_id]['func'], task_graph, ref_id, params)

  queue.put(task.get_id())
  return

def run_task_graph(params, task_graph, unique_id=None):
  # Log the time that the query was received
  query_received_time = query_tools.get_current_time()
  tic = time.perf_counter()

  # Obtain a unique ID if not yet assigned
  if unique_id == None:
      unique_id = query_tools.initialize_query()

  # Initialize task graph
  initialize_task_graph(task_graph, unique_id=unique_id)

  # Prepare multiprocessing queue and task list
  task_ids = []
  processes = []
  mpq = multiprocessing.Queue()

  # Select and launch all zero-order tasks
  is_originator_task = lambda task: True if (task['order'] == 0) else None
  origin_tasks = filter(is_originator_task, task_graph)

  for task in origin_tasks:
    task_args = (mpq, task_graph, task['ref_id'], params)
    p = multiprocessing.Process(target=enqueue_task, args=task_args)

    processes.append(p)
    p.start()

  for p in processes:
    task = mpq.get()
    task_ids.append(task)

  for p in processes:
    p.join()

  toc = time.perf_counter()

  # Finalize the response
  response_object = {
    'status': 'success',
    'unique_ID': unique_id,
    'params': params,
    'data': {
      'task_id': task_ids
    },
    "benchmarks" : {
      "exec_time" : str(toc - tic),
    }
  }

  # Create and return a response
  response = {}
  response['query_ID']        = unique_id
  response['query_received']  = query_received_time
  response['response_object'] = response_object

  return response

###
##    S0002: Utility Functions
###
def initialize_task_graph(task_graph, unique_id=None):
    if (unique_id != None):
        for t in task_graph:
            t['unique_id'] = unique_id

    else:
        # Just get the unique id from the first task
        unique_id = task_graph[0]['unique_id']

    # Get all distinct task types in task graph and count them
    task_types = {}
    for task in task_graph:
        task_type = "{}_{}".format(task['type'], task['order'])
        if not task_type in list(task_types.keys()):
            task_types[task_type] = 1

        else:
            task_types[task_type] += 1

    redis_conn = redis.StrictRedis(host="redis", port=6379, password="", decode_responses=True)

    # Assign total counts and initialize done counts to zero
    for task_type_key in list(task_types.keys()):
        redis_total_count_key = R_TASK_TYPE_TOTAL.format(unique_id, task_type_key)
        redis_done_count_key = R_TASK_TYPE_DONE.format(unique_id, task_type_key)

        redis_tools.setRedisKV(redis_conn, redis_total_count_key, task_types[task_type_key])
        redis_tools.setRedisKV(redis_conn, redis_done_count_key, 0)
    
    return

