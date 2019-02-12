from flask import current_app

import redis
from rq import Queue, Connection

import time
import json
import multiprocessing

from ...common.defs import *
from ...common.task_graph import TaskGraph
from ...common import redis_tools, query_tools

###
##    S0001: Main Service Functions
###
def get_average_speeds(request, unique_id=None):
  print("Received")
  # Log the time that the query was received
  query_received_time = query_tools.get_current_time()
  tic = time.perf_counter()

  # Set up the task_parameters from the request
  params = {
      'start_time'  : int(request.form['start_time']),
      'end_time'    : int(request.form['end_time']),
      'db_info'     : {
        'name'          : request.form['db_name'],
        'host'          : request.form['host'],
        'port'          : request.form['port'],
        'ret_policy'    : request.form['db_ret_policy'],
        'meas'          : request.form['db_meas'],
      },
  }
  rsu_info_list = json.loads(request.form['rsu_list'])
  cluster_data  = json.loads(request.form['cluster_data'])
  rsu_list = [ rsu['rsu_id'] for rsu in rsu_info_list ]

  strategy = 'clustered-collection'
  if 'strategy' in request.form.keys():
      strategy = request.form['strategy']

  # Obtain a unique ID if not yet assigned
  if unique_id == None:
      unique_id = query_tools.initialize_query(len(rsu_list), use_count=False)

  # Generate a task graph
  task_graph = create_task_graph(unique_id, rsu_list, strategy, 
                                 params={ 'cluster_data' : cluster_data })

  # Initialize task graph
  initialize_task_graph(task_graph)

  with open("vas_task_graph.json", "w") as task_graph_file:
      task_graph_file.write(json.dumps(task_graph, indent=4, sort_keys=True))

  with open("vas_task_params.json", "w") as task_params_file:
      task_params_file.write(json.dumps(params, indent=4, sort_keys=True))

  with open("vas_rsu_list.json", "w") as list_file:
      list_file.write(json.dumps(rsu_list, indent=4, sort_keys=True))

  # Prepare multiprocessing queue and task list
  task_ids = []
  processes = []
  mpq = multiprocessing.Queue()

  print("Launching tasks...")
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

  print("Waiting for processes...")
  for p in processes:
    p.join()

  toc = time.perf_counter()
  print("Done.")

  # Finalize the response
  response_object = {
    'status': 'success',
    'unique_ID': unique_id,
    'params': {
      'start_time'  : request.form['start_time'],
      'end_time'    : request.form['end_time'],
      'split_count' : len(rsu_list), 
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
def get_rsu_list():
    rsu_list = []

    try:
      start_time  = 0
      end_time    = int(time.time() * 1000000000)

      resp = query_tools.query_influx_db(start_time, end_time,
                                         influx_db='rsu_id_location',
                                         influx_ret_policy='autogen',
                                         influx_meas='rsu_locations',
                                         host=INFLUX_HOST,
                                         port=INFLUX_PORT)

      resp_data =  json.loads(resp.text)
      rsu_info_list = resp_data['results'][0]['series'][0]['values'] 

      for raw_info in rsu_info_list:
          rsu_info = {
              'rsu_id' : raw_info[4],
              'lon' : float(raw_info[3]),
              'lat' : float(raw_info[2]),
          }

          rsu_list.append(rsu_info)

    except Exception as e:
      rsu_list = []
    
    return rsu_list

def enqueue_task(queue, task_graph, ref_id, params):
  with Connection(redis.from_url(current_app.config['REDIS_URL'])):
    q = Queue(task_graph[ref_id]['node_id'])

    task = q.enqueue(task_graph[ref_id]['func'], task_graph, ref_id, params)

  queue.put(task.get_id())
  return

###
##    S0003: Custom Task Graph
###
class VasTaskGraph(TaskGraph):
    def __init__(self, unique_id):
        TaskGraph.__init__(self, unique_id)
        self.exec_task_funcs = {
            'collection' : 'VASMAP_Tasks.collect_per_rsu_data',
            'processing' : 'VASMAP_Tasks.average_by_rsu',
            'aggregation' : 'VASMAP_Tasks.aggregate_average_speeds',
        }

        self.add_task_funcs = {
            'collection'  : self.add_collect_task,
            'processing'  : self.add_processing_task,
            'aggregation' : self.add_aggregation_task,
        }

        return

    def add_collect_task(self, node_id, seq_id=1, extras=None):
        collect_task = self.create_task( node_id, 'collection', 
                                         'VASMAP_Tasks.collect_rsu_data', 
                                         seq_id=seq_id, 
                                         extras=extras )

        self.task_graph.append(collect_task)
        self.ref_id += 1

        # Return a reference to the last added item in the graph
        return self.task_graph[-1]

    def add_processing_task(self, node_id, seq_id=1, extras=None):
        process_task = self.create_task( node_id, 'processing', 
                                         'VASMAP_Tasks.average_by_rsu', 
                                         seq_id=seq_id, 
                                         extras=extras )


        self.task_graph.append(process_task)
        self.ref_id += 1

        # Return a reference to the last added item in the graph
        return self.task_graph[-1]

    def add_aggregation_task(self, node_id, seq_id=1, extras=None):
        process_task = self.create_task( node_id, 'aggregation', 
                                         'VASMAP_Tasks.aggregate_average_speeds', 
                                         seq_id=seq_id, 
                                         extras=extras )


        self.task_graph.append(process_task)
        self.ref_id += 1

        # Return a reference to the last added item in the graph
        return self.task_graph[-1]

def create_task_graph(unique_id, node_list, strategy, params=None):
    # Instantiate the task graph object
    task_graph_obj = VasTaskGraph(unique_id)

    if strategy == 'single-collector':
        # Assign the nodes
        collect_nodes   = [ node_list[0] ] # TODO this should be random?
        process_nodes   = node_list
        aggregate_nodes = [ node_list[0] ] # TODO this should be random?

        collect_tasks   = task_graph_obj.add_tasks(collect_nodes, 'collection')
        process_tasks   = task_graph_obj.add_tasks(process_nodes, 'processing')
        aggregate_tasks = task_graph_obj.add_tasks(aggregate_nodes, 'aggregation')

        for ct in collect_tasks:
            task_graph_obj.add_next_dest(ct, process_tasks)

        for pt in process_tasks:
            task_graph_obj.add_next_dest(pt, aggregate_tasks)

    elif strategy == 'handle-own-data':
        # Assign the nodes
        collect_nodes = node_list
        process_nodes = node_list
        aggregate_nodes = [ node_list[0] ] # TODO this should be random?

        collect_tasks   = task_graph_obj.add_tasks(collect_nodes, 'collection')
        process_tasks   = task_graph_obj.add_tasks(process_nodes, 'processing')
        aggregate_tasks = task_graph_obj.add_tasks(aggregate_nodes, 'aggregation')

        for ct in collect_tasks:
            # Find the matching node_id in the list of processing tasks
            matched_tasks = []
            for pt in process_tasks:
                if pt['node_id'] == ct['node_id']:
                    matched_tasks.append(pt)
                    break

            if len(matched_tasks) == 0:
                continue

            task_graph_obj.add_next_dest(ct, matched_tasks)

        for pt in process_tasks:
            task_graph_obj.add_next_dest(pt, aggregate_tasks)

    elif strategy == 'clustered-collection':
        cluster_data = params['cluster_data']
        master_node_list = list(cluster_data.keys())
        
        # Readd the master node to each cluster
        for master_rsu in master_node_list:
            cluster_data[master_rsu].append(master_rsu)

        # Assign the nodes
        collect_nodes   = master_node_list # TODO this should be random?
        process_nodes   = node_list
        aggregate_nodes = [ node_list[0] ] # TODO this should be random?

        collect_tasks   = task_graph_obj.add_tasks(collect_nodes, 'collection')
        process_tasks   = task_graph_obj.add_tasks(process_nodes, 'processing')
        aggregate_tasks = task_graph_obj.add_tasks(aggregate_nodes, 'aggregation')

        for ct in collect_tasks:
            # Resolve nodes belonging to this cluster
            cluster_node_ids = cluster_data[ct['node_id']]
            cluster_nodes = []
            for pt in process_tasks:
                if pt['node_id'] in cluster_node_ids:
                    cluster_nodes.append(pt)

            task_graph_obj.add_next_dest(ct, cluster_nodes)

        for pt in process_tasks:
            task_graph_obj.add_next_dest(pt, aggregate_tasks)

    return task_graph_obj.get_task_graph()

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


