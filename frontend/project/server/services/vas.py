from flask import current_app

import redis
from rq import Queue, Connection

import time
import json
import multiprocessing

from ...common.defs import *
from ...common import redis_tools, query_tools

def enqueue_task(queue, task_graph, ref_id, params):
  with Connection(redis.from_url(current_app.config['REDIS_URL'])):
    q = Queue(task_graph[ref_id]['node_id'])

    task = q.enqueue(task_graph[ref_id]['func'], task_graph, ref_id, params)

  queue.put(task.get_id())
  return

def create_task_graph(unique_id, node_list, strategy):
    task_graph = []
    ref_id = 0
    order = 0

    if strategy == 'single-collector':
        # Assign the nodes
        collect_node = node_list[0] # TODO this should be random?
        process_nodes = node_list
        aggregate_node = node_list[0] # TODO this should be random?

        # Create a collection task and add it to the task graph
        collect_dest_tasks = []
        dest_task = { 
            'nodes'   : [ pn for pn in process_nodes ] ,
            'type'    : 'processing',
            'func'    : 'VASMAP_Tasks.average_by_rsu',
            'order'   : order+1,
        }
        collect_dest_tasks.append(dest_task)

        collect_task = {
            'ref_id'        : ref_id,
            'unique_id'     : unique_id,
            'type'          : 'collection',
            'seq_id'        : 1,
            'func'          : 'VASMAP_Tasks.collect_rsu_data',
            'order'         : order,
            'node_id'       : collect_node,
            'dest'          : collect_dest_tasks ,
        }
        task_graph.append(collect_task)
        ref_id += 1
        order += 1

        # Create processing tasks and add them to the task graph
        dest_task = { 
            'nodes'   : [ aggregate_node ],
            'type'    : 'aggregation',
            'func'    : 'VASMAP_Tasks.aggregate_average_speeds',
            'order'   : order+1,
        }

        for seq_id in range(0, len(process_nodes)):
            process_task = {
                'ref_id'        : ref_id,
                'unique_id'     : unique_id,
                'type'          : 'processing',
                'seq_id'        : seq_id,
                'func'          : 'VASMAP_Tasks.average_by_rsu',
                'order'         : order,
                'node_id'       : process_nodes[seq_id],
                'dest'          : [ dest_task ] ,
            }

            task_graph.append(process_task)
            ref_id += 1

        order += 1

        # Create an aggregation task and add it to the task graph
        aggregate_task = {
            'ref_id'        : ref_id,
            'unique_id'     : unique_id,
            'type'          : 'aggregation',
            'seq_id'        : 1,
            'func'          : 'VASMAP_Tasks.aggregate_average_speeds',
            'order'         : order,
            'node_id'       : aggregate_node,
            'dest'          : [],
        }
        task_graph.append(aggregate_task)
        ref_id += 1
        order += 1

    elif strategy == 'handle-own-data':
        # Assign the nodes
        collect_nodes = node_list
        process_nodes = node_list
        aggregate_node = node_list[0] # TODO this should be random?

        seq_id = 0
        for node_id in node_list:
            # Create a collection task and add it to the task graph
            collect_dest_tasks = []
            dest_task = { 
                'nodes'   : [ node_id ], # Route to the same node, but do processing
                'type'    : 'processing',
                'func'    : 'VASMAP_Tasks.average_by_rsu',
                'order'   : order + 1,
            }
            collect_dest_tasks.append(dest_task)
            collect_task = {
                'ref_id'        : ref_id,
                'unique_id'     : unique_id,
                'type'          : 'collection',
                'seq_id'        : seq_id,
                'func'          : 'VASMAP_Tasks.collect_per_rsu_data',
                'order'         : order,
                'node_id'       : node_id,
                'dest'          : collect_dest_tasks ,
            }
            task_graph.append(collect_task)
            ref_id += 1

            # Create the processing task and add it to the task graph
            process_dest_task = { 
                'nodes'   : [ aggregate_node ],
                'type'    : 'aggregation',
                'func'    : 'VASMAP_Tasks.aggregate_average_speeds',
                'order'   : order+2,
            }

            process_task = {
                'ref_id'        : ref_id,
                'unique_id'     : unique_id,
                'type'          : 'processing',
                'seq_id'        : seq_id,
                'func'          : 'VASMAP_Tasks.average_by_rsu',
                'order'         : order+1,
                'node_id'       : node_id,
                'dest'          : [ process_dest_task ] ,
            }

            task_graph.append(process_task)
            ref_id += 1

            seq_id += 1

        order += 2

        # Create an aggregation task and add it to the task graph
        aggregate_task = {
            'ref_id'        : ref_id,
            'unique_id'     : unique_id,
            'type'          : 'aggregation',
            'seq_id'        : 1,
            'func'          : 'VASMAP_Tasks.aggregate_average_speeds',
            'order'         : order,
            'node_id'       : aggregate_node,
            'dest'          : [],
        }
        task_graph.append(aggregate_task)
        ref_id += 1
        order += 1

    return task_graph

def get_average_speeds(request, unique_id=None):
  # Log the time that the query was received
  query_received_time = query_tools.get_current_time()
  tic = time.perf_counter()

  # Parse the request
  req = request.get_json(force=True)

  influx_ip   = req['influx_ip']
  rsu_list    = json.loads(req['rsu_list'])
  start_time  = req['start_time']       # Long Integer
  end_time    = req['end_time']         # Long Integer
  split_count = len(rsu_list) # Integer

  strategy = 'handle-own-data'
  if 'strategy' in req.keys():
      strategy = req['strategy']

  # Obtain a unique ID if not yet assigned
  if unique_id == None:
      unique_id = query_tools.initialize_query(split_count, use_count=False)

  # Set up the task_parameters
  if influx_ip == "" or influx_ip == "DEFAULT":
    influx_ip = '163.221.68.233'

  params = {
      'start_time' : start_time,
      'end_time' : end_time,
      'db_info' : {
        'name' : 'rsu_speed',
        'host' : influx_ip,
        'port' : '8086',
        'ret_policy' : 'autogen',
        'meas' : 'rsu_speeds',
      },
  }

  # Generate a task graph
  task_graph = create_task_graph(unique_id, rsu_list, strategy)

  # Initialize task graph
  initialize_task_graph(task_graph)

  with open("vas_task_graph.json", "w") as task_graph_file:
      task_graph_file.write(json.dumps(task_graph, indent=4, sort_keys=True))

  with open("vas_task_params.json", "w") as task_params_file:
      task_params_file.write(json.dumps(params, indent=4, sort_keys=True))

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
    'params': {
      'start_time'  : start_time,
      'end_time'    : end_time,
      'split_count' : split_count
    },
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
def initialize_task_graph(task_graph):
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

    redis_conn = redis.StrictRedis(host=REDIS_HOST, port=REDIS_PORT, password="", decode_responses=True)

    # Assign total counts and initialize done counts to zero
    for task_type_key in list(task_types.keys()):
        redis_total_count_key = R_TASK_TYPE_TOTAL.format(unique_id, task_type_key)
        redis_done_count_key = R_TASK_TYPE_DONE.format(unique_id, task_type_key)

        redis_tools.setRedisKV(redis_conn, redis_total_count_key, task_types[task_type_key])
        redis_tools.setRedisKV(redis_conn, redis_done_count_key, 0)
    
    return

def get_rsu_list():
    rsu_list = []

    start_time  = 0
    end_time    = int(time.time() * 1000000000)

    resp = query_tools.query_influx_db(start_time, end_time,
                                       influx_db='rsu_id_location',
                                       influx_ret_policy='autogen',
                                       influx_meas='rsu_locations',
                                       host='163.221.68.206',
                                       port='8086')

    resp_data =  json.loads(resp.text)
    rsu_info_list = resp_data['results'][0]['series'][0]['values'] 

    for raw_info in rsu_info_list:
        rsu_info = {
            'rsu_id' : raw_info[4],
            'lon' : float(raw_info[3]),
            'lat' : float(raw_info[2]),
        }

        rsu_list.append(rsu_info)
    
    return rsu_list



