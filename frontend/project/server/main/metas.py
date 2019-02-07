import json

import redis
from rq import Queue, Connection
from rq.registry import StartedJobRegistry, FinishedJobRegistry

from ...common.defs import *

def get_task_status(queue, task_id, app_info):
  task = None
  with Connection(redis.from_url(app_info['REDIS_URL'])):
    q = Queue(queue)
    task = q.fetch_job(task_id)

  if task is not None:
    response_object = {
      'status': 'success',
      'data': {
          'task_id': task.get_id(),
          'task_status': task.get_status(),
          'task_result': task.result,
      }
    }

  else:
    response_object = {'status': 'error, task is None'}

  return response_object

def get_meta_info(app_info):
  with Connection(redis.from_url(app_info['REDIS_URL'])):
    q = Queue('default')
    registry = StartedJobRegistry('default')
    f_registry = FinishedJobRegistry('default')

    # Retrieve task ids
    all_task_ids = get_all_task_ids(app_info)

    task_data = {
      'queue' : q,
      'task_ids'   : all_task_ids,
    }

    data = {}
    data['running_tasks'] = fetch_tasks_by_category(task_data, "running")
    data['queued_tasks'] = fetch_tasks_by_category(task_data, "queued")
    data['finished_tasks'] = fetch_tasks_by_category(task_data, "finished")

  with Connection(redis.from_url(app_info['REDIS_URL'])):
    q = Queue('aggregator')

    # Get all aggregated finished tasks
    task_data = {
      'queue' : q,
      'task_ids' : get_all_finished_tasks_from('aggregator', app_info),
    }
    data['agg_finished_tasks'] = fetch_tasks_by_category(task_data, "finished")

    # Get all aggregated queued tasks
    task_data = {
      'queue' : q,
      'task_ids' : get_all_queued_tasks_from('aggregator', app_info),
    }
    data['agg_queued_tasks'] = fetch_tasks_by_category(task_data, "queued")

    # Get all aggregated running tasks
    task_data = {
      'queue' : q,
      'task_ids' : get_all_running_tasks_from('aggregator', app_info),
    }
    data['agg_running_tasks'] = fetch_tasks_by_category(task_data, "running")

    return data

def generate_task_info(queue_name, job_ids, category):
    info = {}
    info['status'] = 'success'
    info['queue_name'] = queue_name
    info[category] = {}

    info[category]['count'] = len(job_ids)
    task_list_key = '{}_tasks_ids'.format(category)
    info[category][task_list_key] = []

    for job_id in job_ids:
        info[category][task_list_key].append(job_id)

    return info

def get_all_finished_tasks_from(queue_name, app_info):
    with Connection(redis.from_url(app_info['REDIS_URL'])):
      f_registry = FinishedJobRegistry(queue_name)
      job_ids = f_registry.get_job_ids()

      # Generate the task info
      return generate_task_info(queue_name, job_ids, "finished")

def get_all_queued_tasks_from(queue_name, app_info):
    with Connection(redis.from_url(app_info['REDIS_URL'])):
      q = Queue(queue_name)
      job_ids = q.job_ids

      # Generate the task info
      return generate_task_info(queue_name, job_ids, "queued")

def get_all_running_tasks_from(queue_name, app_info):
    with Connection(redis.from_url(app_info['REDIS_URL'])):
      registry = StartedJobRegistry(queue_name)
      job_ids = registry.get_job_ids()

      # Generate the task info
      return generate_task_info(queue_name, job_ids, "running")

def get_all_task_ids(app_info):
  with Connection(redis.from_url(app_info['REDIS_URL'])):
    q = Queue('default')
    registry = StartedJobRegistry('default')
    f_registry = FinishedJobRegistry('default')

  if q:
    data = {}
    running_job_ids = registry.get_job_ids()
    expired_job_ids = registry.get_expired_job_ids()
    finished_job_ids = f_registry.get_job_ids()
    queued_job_ids = q.job_ids
    data['status'] = 'success'
    data['queue_name'] = 'default' #Make dynamic or parameterized?
    data['running'] = {}
    data['queued'] = {}
    data['expired'] = {}
    data['finished'] = {}

    data['running']['count'] = len(running_job_ids)
    data['running']['running_tasks_ids'] = []
    for running_job_id in running_job_ids:
      data['running']['running_tasks_ids'].append(running_job_id)

    data['queued']['count'] = len(queued_job_ids)
    data['queued']['queued_tasks_ids'] = []
    for queued_job_id in queued_job_ids:
      data['queued']['queued_tasks_ids'].append(queued_job_id)

    data['expired']['count'] = len(expired_job_ids)
    data['expired']['expired_tasks_ids'] = []
    for expired_job_id in expired_job_ids:
      data['expired']['expired_tasks_ids'].append(expired_job_id)

    data['finished']['count'] = len(finished_job_ids)
    data['finished']['finished_tasks_ids'] = []
    for finished_job_id in finished_job_ids:
      data['finished']['finished_tasks_ids'].append(finished_job_id)

    return data

  return {'status': 'error'}

def fetch_tasks_by_category(task_data, category):
  rqueue = task_data['queue']
  all_task_ids = task_data['task_ids']

  task_list_label = "{}_tasks_ids".format(category)
  filtered_ids = all_task_ids[category][task_list_label]

  data = []
  for task_id in filtered_ids:
    d = {}
    job = rqueue.fetch_job(task_id)
    job.refresh()
    job.meta['result'] = 'null'
    d[task_id] = job.meta
    data.append(d)

  return data

def add_exec_time_info(unique_id, operation, time_start, time_end):
  r = redis.StrictRedis(host="redis", port=6379, password="", decode_responses=True)

  # Add the unique_id to the execution time info list if it does not yet exist
  r.sadd(EXEC_TIME_INFO, unique_id)

  # Push an operation to the execution time log for this unique_id
  log_obj = {
    'operation'   : operation,
    'start_time'  : str(time_start),
    'end_time'    : str(time_end),
    'duration'    : str(float(time_end - time_start) / 1000000.0),
  }
  r.lpush("{}_{}".format(EXEC_TIME_INFO, unique_id), json.dumps(log_obj))

  return True

def get_exec_time_log_ids():
  r = redis.StrictRedis(host="redis", port=6379, password="", decode_responses=True)
  return r.smembers(EXEC_TIME_INFO)

  return None

def get_exec_time_log(unique_id):
  try:
    r = redis.StrictRedis(host="redis", port=6379, password="", decode_responses=True)
    list_id = "{}_{}".format(EXEC_TIME_INFO, unique_id)
    #return r.get("{}_{}".format(EVENT_LOGS, unique_id))
    item_count = r.llen(list_id)

    logs = []
    for i in range(0, item_count):
      logs.append(json.loads(r.lindex(list_id, i)))

    return logs

  except Exception as e:
    pass

  return []

def get_all_exec_time_logs():
  exec_time_log_ids = get_exec_time_log_ids()

  logs = {}
  for uid in exec_time_log_ids:
    logs[uid] = get_exec_time_log(uid)

  return logs

