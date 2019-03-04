import redis
import rq
import time
import json
import socket

from common.defs import *
from common import redis_tools
from common import metas as meta_tools
from common.mqtt_tools import MqttLog

class AttrDict(dict):
  def __init__(self, *args, **kwargs):
    super(AttrDict, self).__init__(*args, **kwargs)
    self.__dict__ = self
    return

class BackendTask():
  def __init__( self, task_type,
                      task_func=None,
                      pre_exec_func=None,
                      post_exec_func=None ):

    self.task_func = task_func
    self.task_pre_exec_func = pre_exec_func
    self.task_post_exec_func = post_exec_func

    self.attr = AttrDict()
    self.attr['task_type']      = task_type
    self.attr['task_type_id']   = None
    self.attr['task_info']      = None
    self.attr['unique_id']      = None
    self.attr['job']            = None

    self.mqtt_log               = None
  
    return
  
  def run(self, task_graph, reference_id, params):
    # Execute pre exec function
    if self.task_pre_exec_func != None:
      self.task_pre_exec_func(task_graph, reference_id, params)
  
    # Resolve task info and parameters
    self.attr.task_info     = task_graph[reference_id]
    self.attr.unique_id     = self.attr.task_info['unique_id']
    self.attr.task_type_id  = "{}_{}".format(self.attr.task_info['type'], self.attr.task_info['order'])
  
    # Load this node's information
    node_id       = None
    with open("./data/node_info.json") as node_info_file:
      # Load node identity from "./data/node_info.json"
      node_info = json.load(node_info_file)
      node_id = node_info['node_id']
  
    # Notify queue of job pick up
    self.attr.job = rq.get_current_job()
    self.notify_job_pick_up(self.attr.job, self.attr.unique_id, self.attr.task_info)
  
    # Log the beginning of the task
    self.mqtt_log = MqttLog(node_id, self.attr.unique_id)
    self.log_event(self.attr.task_type, 'started')
    task_start_time = redis_tools.get_redis_server_time()
    tic = time.perf_counter()
  
    # Add handled job to execution list
    self.notify_job_exec_start(self.attr.job, self.attr.unique_id, node_id, self.attr.task_info)
  
    # Perform the actual task
    if self.task_func != None:
      task_results = self.task_func(task_graph, reference_id, params, 
                                    node_id=node_id, task_attr=self.attr)
  
    # Update job progress
    toc = time.clock()
    self.notify_job_exec_end(self.attr.job, self.attr.unique_id, node_id, self.attr.task_info, tic, toc)
  
    # If this is not the terminal job, then do metadata checking / creation
    if not 'last_job' in list(task_results.keys()):
      # Connect to Redis
      redis_conn = redis.StrictRedis(host=REDIS_HOST, port=REDIS_PORT, password="", decode_responses=True)
  
      # Retrieve the task counts for this
      redis_done_count_key  = R_TASK_TYPE_DONE.format(self.attr.unique_id, self.attr.task_type_id)
      redis_total_count_key = R_TASK_TYPE_TOTAL.format(self.attr.unique_id, self.attr.task_type_id)
      task_count            = redis_tools.getRedisV(redis_conn, redis_total_count_key)
      done_task_count       = redis_tools.getRedisV(redis_conn, redis_done_count_key)
  
      # Create metadata
      metas = {
        'job_id' : self.attr.job.id,
        'unique_id' : self.attr.unique_id,
        'task_count' : task_count,
        'done_task_count' : done_task_count,
      }

      # Add in extra metadata if needed
      for k in list(task_results['metas'].keys()):
          metas[k] = task_results['metas'][k]
  
      # TODO Delay
      # Execute post exec function
      if self.task_post_exec_func != None:
        post_exec_results = self.task_post_exec_func(task_graph, reference_id, params, 
                                                     node_id=node_id, metas=metas, task_attr=self.attr)

        # Add in extra metadata if needed
        for k in list(post_exec_results['metas'].keys()):
            metas[k] = post_exec_results['metas'][k]
  
      # Log execution time info to redis
      task_end_time = redis_tools.get_redis_server_time()
      meta_tools.add_exec_time_info(self.attr.unique_id, 
                                    "{}-{}".format(self.attr.task_type_id, self.attr.task_info['seq_id']), 
                                    task_start_time, task_end_time)
      self.log_event(self.attr.task_type, "finished")
      self.log_exec_time(self.attr.task_type, task_start_time, task_end_time)
  
      print("Logging task results...")
      # Log partial results
      self.log_results(task_results['output'], subtype=self.attr.task_type, metas=metas)

      return { 'sequence_ID': self.attr.task_info['seq_id'], 
               'metas' : metas, 
               'output': task_results['output'], 
               'outsize': task_results['outsize'] if ('outsize' in task_results) else 0, }

    # If this IS the terminal job, then skip metadata checking / creation
    # Log execution time info to redis
    task_end_time = redis_tools.get_redis_server_time()
    self.task_type_id  = "{}_{}".format(self.attr.task_info['type'], self.attr.task_info['order'])
    meta_tools.add_exec_time_info(self.attr.unique_id, 
                                  "{}-{}".format(self.attr.task_type_id, self.attr.task_info['seq_id']), 
                                  task_start_time, task_end_time)
    self.log_event(self.attr.task_type, "finished")
    self.log_exec_time(self.attr.task_type, task_start_time, task_end_time)

    print("Logging task results...")
    # Log the final result
    self.log_results(task_results['result'], subtype=self.attr.task_type)

    return task_results['result']

  def notify_job_pick_up(self, job, unique_id, task_info):
    # Update job meta information
    job.meta['handled_by'] = socket.gethostname()
    job.meta['handled_time'] = int(time.time())
    job.meta['progress'] = 0.0
    job.meta['unique_id'] = unique_id
    job.meta['task_type'] = task_info['type']
    job.meta['task_index'] = task_info['seq_id']
    job.meta['queue_id'] = task_info['node_id']
  
    job.save_meta()
  
    # Update redis task entry
    task_info = {
      'task_id'       : job.id,
      'ref_id'        : task_info['ref_id'],
      'unique_id'     : job.meta['unique_id'],
      'type'          : job.meta['task_type'],
      'index'         : job.meta['task_index'],
      'queue_id'      : job.meta['queue_id'],
      'handled_by'    : job.meta['handled_by'],
      'handled_time'  : job.meta['handled_time'],
      'progress'      : job.meta['progress'],
      'dest'          : task_info['dest'],
    }
  
    # Connect to Redis
    redis_conn = redis.StrictRedis(host=REDIS_HOST, port=REDIS_PORT, password="", decode_responses=True)
  
    # Save task info to redis
    redis_task_info_key = R_TASK_INFO.format(unique_id, job.id)
    redis_conn.set(redis_task_info_key, json.dumps(task_info))
  
    return

  #def __call__(self, _func=None):
  #  def decorator(task_func):
  #    print("Executing {}".format(task_func))

  #    @functools.wraps(task_func)
  #    def run_task(task_graph, reference_id, params):
  #      # Execute pre exec function
  #      if self.task_pre_exec_func != None:
  #        self.task_pre_exec_func(task_graph, reference_id, params)
  #    
  #      # Resolve task info and parameters
  #      self.attr.task_info     = task_graph[reference_id]
  #      self.attr.unique_id     = self.attr.task_info['unique_id']
  #      self.attr.task_type_id  = "{}_{}".format(self.attr.task_info['type'], self.attr.task_info['order'])
  #    
  #      # Load this node's information
  #      node_id       = None
  #      with open("./data/node_info.json") as node_info_file:
  #        # Load node identity from "./data/node_info.json"
  #        node_info = json.load(node_info_file)
  #        node_id = node_info['node_id']
  #    
  #      # Notify queue of job pick up
  #      self.attr.job = rq.get_current_job()
  #      self.notify_job_pick_up(self.attr.job, self.attr.unique_id, self.attr.task_info)
  #    
  #      # Log the beginning of the task
  #      log = MqttLog(node_id, self.attr.unique_id)
  #      log.event(self.attr.task_type, 'started')
  #      task_start_time = redis_tools.get_redis_server_time()
  #      tic = time.perf_counter()
  #    
  #      # Add handled job to execution list
  #      self.notify_job_exec_start(self.attr.job, self.attr.unique_id, node_id, self.attr.task_info)
  #    
  #      # Perform the actual task
  #      task_results = task_func(task_graph, reference_id, params, 
  #                               node_id=node_id, task_attr=self.attr)
  #    
  #      # Update job progress
  #      toc = time.clock()
  #      self.notify_job_exec_end(self.attr.job, self.attr.unique_id, node_id, self.attr.task_info, tic, toc)

  #      print(task_results)
  #    
  #      # If this is not the terminal job, then do metadata checking / creation
  #      if not 'last_job' in list(task_results.keys()):
  #        # Connect to Redis
  #        redis_conn = redis.StrictRedis(host=REDIS_HOST, port=REDIS_PORT, password="", decode_responses=True)
  #    
  #        # Retrieve the task counts for this
  #        redis_done_count_key  = R_TASK_TYPE_DONE.format(self.attr.unique_id, self.attr.task_type_id)
  #        redis_total_count_key = R_TASK_TYPE_TOTAL.format(self.attr.unique_id, self.attr.task_type_id)
  #        task_count            = redis_tools.getRedisV(redis_conn, redis_total_count_key)
  #        done_task_count       = redis_tools.getRedisV(redis_conn, redis_done_count_key)
  #    
  #        # Create metadata
  #        metas = {
  #          'job_id' : self.attr.job.id,
  #          'unique_id' : self.attr.unique_id,
  #          'task_count' : task_count,
  #          'done_task_count' : done_task_count,
  #        }

  #        # Add in extra metadata if needed
  #        for k in list(task_results['metas'].keys()):
  #            metas[k] = task_results['metas'][k]
  #    
  #        # TODO Delay
  #        # Execute post exec function
  #        if self.task_post_exec_func != None:
  #          post_exec_results = self.task_post_exec_func(task_graph, reference_id, params, 
  #                                                       node_id=node_id, metas=metas, task_attr=self.attr)

  #          # Add in extra metadata if needed
  #          for k in list(post_exec_results['metas'].keys()):
  #              metas[k] = post_exec_results['metas'][k]
  #    
  #        # Log execution time info to redis
  #        task_end_time = redis_tools.get_redis_server_time()
  #        meta_tools.add_exec_time_info(self.attr.unique_id, 
  #                                      "{}-{}".format(self.attr.task_type_id, self.attr.task_info['seq_id']), 
  #                                      task_start_time, task_end_time)
  #        log.event(self.attr.task_type, "finished")
  #        log.exec_time(self.attr.task_type, task_start_time, task_end_time)
  #    
  #        # Log partial results
  #        log.results(task_results['output'], subtype=self.attr.task_type, metas=metas)

  #        return { 'sequence_ID': self.attr.task_info['seq_id'], 
  #                 'metas' : metas, 
  #                 'output': task_results['output'], 
  #                 'outsize': task_results['outsize'] if ('outsize' in task_results) else 0, }

  #      # If this IS the terminal job, then skip metadata checking / creation
  #      # Log execution time info to redis
  #      task_end_time = redis_tools.get_redis_server_time()
  #      self.task_type_id  = "{}_{}".format(self.attr.task_info['type'], self.attr.task_info['order'])
  #      meta_tools.add_exec_time_info(self.attr.unique_id, 
  #                                    "{}-{}".format(self.attr.task_type_id, self.attr.task_info['seq_id']), 
  #                                    task_start_time, task_end_time)
  #      log.event(self.attr.task_type, "finished")
  #      log.exec_time(self.attr.task_type, task_start_time, task_end_time)

  #      # Log the final result
  #      log.results(task_results['result'], subtype=self.attr.task_type)

  #      return task_results['result']

    def log_event(self, event_type, status):
      return self.mqtt_log.event(event_type, status)

    def log_exec_time(self, event_type, start, end):
      return self.mqtt_log.exec_time(event_type, start, end)

    def log_results(self, results, subtype=None, metas=None):
      return self.mqtt_log.results(results, subtype=subtype, metas=metas)

    def notify_job_pick_up(self, job, unique_id, task_info):
      # Update job meta information
      job.meta['handled_by'] = socket.gethostname()
      job.meta['handled_time'] = int(time.time())
      job.meta['progress'] = 0.0
      job.meta['unique_id'] = unique_id
      job.meta['task_type'] = task_info['type']
      job.meta['task_index'] = task_info['seq_id']
      job.meta['queue_id'] = task_info['node_id']
    
      job.save_meta()
    
      # Update redis task entry
      task_info = {
        'task_id'       : job.id,
        'ref_id'        : task_info['ref_id'],
        'unique_id'     : job.meta['unique_id'],
        'type'          : job.meta['task_type'],
        'index'         : job.meta['task_index'],
        'queue_id'      : job.meta['queue_id'],
        'handled_by'    : job.meta['handled_by'],
        'handled_time'  : job.meta['handled_time'],
        'progress'      : job.meta['progress'],
        'dest'          : task_info['dest'],
      }
    
      # Connect to Redis
      redis_conn = redis.StrictRedis(host=REDIS_HOST, port=REDIS_PORT, password="", decode_responses=True)
    
      # Save task info to redis
      redis_task_info_key = R_TASK_INFO.format(unique_id, job.id)
      redis_conn.set(redis_task_info_key, json.dumps(task_info))
    
      return
  
  def notify_job_exec_start(self, job, unique_id, worker_id, task_info):
    # Connect to Redis
    redis_conn = redis.StrictRedis(host=REDIS_HOST, port=REDIS_PORT, password="", decode_responses=True)
  
    # Push handled job to the redis execution list for this worker
    redis_task_list_key = R_TASKS_LIST.format(unique_id)
    redis_tools.appendToListK(redis_conn, redis_task_list_key, job.id )
  
    return
  
  def notify_job_exec_end(self, job, unique_id, worker_id, task_info, task_start, task_end):
    task_type_id  = "{}_{}".format(task_info['type'], task_info['order'])
  
    # Connect to Redis
    redis_conn = redis.StrictRedis(host=REDIS_HOST, port=REDIS_PORT, password="", decode_responses=True)
  
    # Update job progress
    job.meta['progress'] = task_end - task_start
    job.save_meta()
  
    # Increment finished nodes counter
    redis_done_count_key = R_TASK_TYPE_DONE.format(unique_id, task_type_id)
    redis_conn.incr(redis_done_count_key)
  
    return



