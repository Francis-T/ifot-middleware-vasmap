import os
import redis
import rq
import time
import socket
import json
import requests
import pandas as pd
import numpy as np
import datetime as dt
import multiprocessing

from rq import Queue, Connection
from common.defs import *
from common import redis_tools
from common import metas as meta_tools
from common.mqtt_tools import MqttLog
from tools.backend_task import BackendTask

###
##    S0001: Main Functions
###
def collect_rsu_data(task_graph, reference_id, params):
  collect_task = CollectTask()
  return collect_task.run(task_graph, reference_id, params)

def collect_per_rsu_data(task_graph, reference_id, params):
  collect_per_rsu_task = CollectPerRsuTask()
  return collect_per_rsu_task.run(task_graph, reference_id, params)

def average_by_rsu(task_graph, reference_id, params):
  average_by_rsu_task = AverageByRsuTask()
  return average_by_rsu_task.run(task_graph, reference_id, params)

def aggregate_average_speeds(task_graph, reference_id, params):
  aggregate_speeds_task = AggregateAverageSpeedsTask()
  return aggregate_speeds_task.run(task_graph, reference_id, params)

###
##    S0002: Utility Functions
###
def enqueue_task(queue, task_graph, ref_id, params):
  with Connection(redis.from_url(REDIS_URL)):
    q = Queue(task_graph[ref_id]['node_id'])

    task = q.enqueue(task_graph[ref_id]['func'], task_graph, ref_id, params)

  queue.put(task.get_id())
  return

def error_result(message):
  return { 'unique_id': unique_id, 
           'metas' : { 'status' : 'failed', 'message' : message }  }

def query_influx_db(start, end, fields="*",
                                influx_db='IFoT-GW2',
                                influx_ret_policy='autogen',
                                influx_meas='IFoT-GW2-Meas',
                                host=INFLUX_HOST,
                                port=INFLUX_PORT,
                                rsu_id=None):

    # Build the filter clause
    where = ""
    if start < EXPECTED_TIME_RANGE:
      start = int(start) * NANO_SEC_ADJUSTMENT

    if end < EXPECTED_TIME_RANGE:
      end = int(end) * NANO_SEC_ADJUSTMENT

    source = '"{}"."{}"."{}"'.format(influx_db, influx_ret_policy, influx_meas)
    where  = 'WHERE time >= {} AND time <= {}'.format(start, end)
    if rsu_id != None:
      where += " AND rsu_id = '{}'".format(rsu_id)

    query = "SELECT {} from {} {} LIMIT 1000".format(fields, source, where)

    payload = {
        "db": influx_db,
        "pretty": True,
        "epoch": 'ms',
        "q": query
    }

    influx_url = "http://{}:{}/query".format(host, port)
    return requests.get(influx_url, params=payload)


###
##    S0004: VASMAP Task Classes
###
class CollectTask(BackendTask):
  def __init__(self):
    BackendTask.__init__(self, 'collection', task_func=self.do_task)
    return

  def do_task(self, task_graph, reference_id, params, node_id=None):
    # Resolve parameters
    db_info       = params['db_info']
    start_time    = params['start_time']
    end_time      = params['end_time']

    # Retrieve the data from the InfluxDB
    resp = query_influx_db( start_time, end_time,
                            host=db_info['host'],
                            port=db_info['port'],
                            influx_db=db_info['name'],
                            influx_ret_policy=db_info['ret_policy'],
                            influx_meas=db_info['meas'])

    # Split into columns and values
    columns = json.loads(resp.text)['results'][0]['series'][0]['columns']
    values =  json.loads(resp.text)['results'][0]['series'][0]['values']

    # Load the data bound for each destination node
    df = pd.DataFrame(values, columns=columns)

    all_dest_nodes = []
    for dest_info in self.task_info['dest']: # TODO
      for dest_node in dest_info['nodes']:
          if dest_node in all_dest_nodes:
              continue

          all_dest_nodes.append(dest_node)

    node_df = df[df['rsu_id'].isin(all_dest_nodes)]
    params = {
      'columns' : list(node_df.columns.values),
      'values'  : list(node_df.values),
      'db_info' : db_info,
    }

    dest_node_data_list = []
    for dest_info in self.task_info['dest']: # TODO
      for dest_node in dest_info['nodes']:
          # Get the matching task in the task graph
          dest_task = None
          for task in task_graph:
              if task['node_id'] == dest_node and \
                 task['type'] == dest_info['type'] and \
                 task['order'] == dest_info['order']:
                
                dest_task = task
                break

          if dest_task == None: 
              continue

          dest_node_data = {
            'data' : dest_task,
            'params' : params,
          }
          dest_node_data_list.append(dest_node_data)

    print([ d['data'] for d in dest_node_data_list ])
    # Route the data to each destination node
    mpq = multiprocessing.Queue()
    processes = []
    for dest_node in dest_node_data_list:
      task_args = (mpq, task_graph, dest_node['data']['ref_id'], dest_node['params'])
      p = multiprocessing.Process(target=enqueue_task, args=task_args)
      processes.append(p)
      p.start()

    for p in processes:
      p.join()

    return { 'output' : [ d['data'] for d in dest_node_data_list ], 
             'outsize' : len(dest_node_data_list), 'metas' : {} }

class CollectPerRsuTask(BackendTask):
  def __init__(self):
    BackendTask.__init__(self, 'collection', task_func=self.do_task)
    return

  def do_task(self, task_graph, reference_id, params, node_id=None):
    # Resolve parameters
    db_info       = params['db_info']
    start_time    = params['start_time']
    end_time      = params['end_time']

    # Retrieve the data from the InfluxDB
    resp = query_influx_db( start_time, end_time,
                            host=db_info['host'],
                            port=db_info['port'],
                            influx_db=db_info['name'],
                            influx_ret_policy=db_info['ret_policy'],
                            influx_meas=db_info['meas'])

    # Split into columns and values
    columns = json.loads(resp.text)['results'][0]['series'][0]['columns']
    values =  json.loads(resp.text)['results'][0]['series'][0]['values']

    # Load the data bound for each destination node
    df = pd.DataFrame(values, columns=columns)
    dest_node_data_list = []
    for dest_info in task_info['dest']: # TODO
      for dest_node in dest_info['nodes']:
          node_df = df[df['rsu_id'] == dest_node]
          params = {
            'columns' : list(node_df.columns.values),
            'values'  : list(node_df.values),
            'db_info' : db_info,
          }

          # Get the matching task in the task graph
          dest_task = None
          for task in task_graph:
              if task['node_id'] == dest_node and \
                 task['type'] == dest_info['type'] and \
                 task['order'] == dest_info['order']:
                
                dest_task = task
                break

          if dest_task == None: 
              continue

          dest_node_data = {
            'data' : dest_task,
            'params' : params,
          }
          dest_node_data_list.append(dest_node_data)

    # Route the data to each destination node
    mpq = multiprocessing.Queue()
    processes = []

    for dest_node in dest_node_data_list:
      task_args = (mpq, task_graph, dest_node['data']['ref_id'], dest_node['params'])
      p = multiprocessing.Process(target=enqueue_task, args=task_args)
      processes.append(p)
      p.start()

    for p in processes:
      p.join()

    return { 'output' : dest_node_data_list, 'outsize' : len(dest_node_data_list), 'metas' : {} }

class AverageByRsuTask(BackendTask):
  def __init__(self):
    BackendTask.__init__(self, 'processing', task_func=self.do_task, post_exec_func=self.do_post_task)
    return

  def do_task(self, task_graph, reference_id, params, node_id=None):
    # Resolve parameters
    db_info       = params['db_info']
    columns       = params['columns']
    values        = params['values']

    # Load the data
    df = pd.DataFrame(values, columns=columns)

    # Get the sum of all speeds by RSU ID how many were summed in each step
    agg_speeds = df.groupby(['rsu_id'], as_index=False)['speed'].sum()
    agg_speeds['count'] = df.groupby(['rsu_id'], as_index=False)['speed'].count()['speed']

    results = {
      'aggregated_speeds' : agg_speeds.values.tolist(),
    }

    return { 'output' : results, 'outsize' : len(results['aggregated_speeds']), 'metas' : {} }

  def do_post_task(self, task_graph, reference_id, params, node_id=None, metas=None):
    extra_metas = {}

    # Resolve parameters
    task_count      = metas['task_count']
    done_task_count = metas['done_task_count']

    # Connect to Redis
    redis_conn = redis.StrictRedis(host=REDIS_HOST, port=REDIS_PORT, password="", decode_responses=True)

    #TODO: Check if result is not yet done before aggregation
    #http://python-rq.org/docs/ --> need to wait a while until the worker is finished
    if task_count == done_task_count:
      redis_tools.setRedisKV(redis_conn, self.unique_id, "finished")

      # Retrieve the next task
      next_tasks = []
      for t in task_graph:
        dest_tasks = self.task_info['dest'] # TODO

        is_next_task = False
        for dt in dest_tasks:
          if not t['node_id'] in dt['nodes']:
              continue

          if not t['type'] == dt['type']:
              continue

          if not t['order'] == dt['order']:
              continue

          is_next_task = True
          break
    
        if is_next_task:
          # If all conditions are satisfied, add this to the next task list
          next_tasks.append(t)

      with Connection(redis_conn):
        for next_task in next_tasks:
          #Maybe add a differnetname?
          q = Queue(next_task['node_id'])
          t = q.enqueue(next_task['func'], task_graph, next_task['ref_id'], None, depends_on=self.job.id) #job is this current job
          extra_metas['agg_task_id'] = t.id

    return { 'metas' : extra_metas }

class AggregateAverageSpeedsTask(BackendTask):
  def __init__(self):
    BackendTask.__init__(self, 'aggregation', task_func=self.do_task)
    return

  def do_task(self, task_graph, reference_id, params, node_id=None):
    # TODO Get status of the task processing
    redis_conn = redis.StrictRedis(host=REDIS_HOST, port=REDIS_PORT, password="", decode_responses=True)
    status = redis_tools.getRedisV(redis_conn, self.unique_id)

    if status != "finished":
      return {'last_job' : True, 'result' : None }

    with Connection(redis.from_url(REDIS_URL)):
      # Get a list of task ids
      task_id_list_all = redis_tools.getListK(redis_conn, R_TASKS_LIST.format(self.unique_id))

      # Resolve information about each task given their task ids
      task_list = []
      for task_id in task_id_list_all:
        json_task_info = redis_tools.getRedisV(redis_conn, R_TASK_INFO.format(self.unique_id, task_id))
        finished_task = json.loads(json_task_info)

        # Check if the finished task directly targets this node
        for dest_task in finished_task['dest']: # TODO

          if ( (self.task_info['node_id'] in dest_task['nodes']) or ('default' in dest_task['nodes']) ) and \
             dest_task['type'] == self.task_info['type'] and \
             dest_task['order'] == self.task_info['order']:

            task_list.append(finished_task)
            break

      # Create a list connecting task ids to their assigned queue ids
      task_to_queue_list = [ { 'task_id' : t['task_id'], 'queue_id' : t['queue_id'] } for t in task_list ]

      #Checking sequence just in case, but costs another for loop
      agg_result = {}
      for queued_task in task_to_queue_list:
        q = Queue(queued_task['queue_id'])
        task = q.fetch_job(queued_task['task_id'])

        if task is not None:
          sequence_ID = task.result["sequence_ID"]

          for result in task.result["output"]["aggregated_speeds"]:
            rsu_id = str(result[0])
            # direction = str(result[1])
            speed = float(result[1])
            count = int(result[2])

            if not rsu_id in agg_result:
              agg_result[rsu_id] = {'speed' : 0, 'count' : 0}

            agg_result[rsu_id]['speed'] += speed
            agg_result[rsu_id]['count'] += count


      for rsu_id in agg_result.keys():
        speed_info = agg_result[rsu_id]
        agg_result[rsu_id] = speed_info['speed'] / speed_info['count']

      d = { 'result': agg_result,
            'unique_id': self.unique_id,
            # 'done_task_count': done_task_count,
            'node_task_id_list': task_to_queue_list }

      return {'last_job' : True, 'result' : d }

