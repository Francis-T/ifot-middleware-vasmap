import time
from rq import get_current_job, Queue, Connection
import socket

import pandas as pd
import numpy as np

from sklearn import svm
from sklearn.externals import joblib

from io import StringIO

import time
import datetime
from dateutil import tz
import redis
import collections
import csv
import json
import paho.mqtt.client as mqtt

from common.defs import *
from common import redis_tools, metas
import config

def aggregate_iris(unique_id):
  print(unique_id)
  aggregation_start_time = redis_tools.get_redis_server_time()
  tic = time.clock()
  job = get_current_job()
  job.meta['handled_by'] = socket.gethostname()
  job.meta['handled_time'] = int(time.time())

  job.meta['progress'] = 0.0
  redis_connection = redis.StrictRedis(host=config.REDIS['host'], 
                                       port=config.REDIS['port'], 
                                       password="", decode_responses=False)

  task_count = redis_tools.getRedisV(redis_connection, unique_id + TASK_COUNT)
  done_task_count = redis_tools.getRedisV(redis_connection, unique_id + DONE_TASK_COUNT)
  status = redis_tools.getRedisV(redis_connection, unique_id).strip().decode('utf-8')

  if status == "finished":
    with Connection(redis_connection):
      node_task_id_list = redis_tools.getListK(redis_connection, unique_id + TASK_SUFFIX)
      node_list_string = [node_task_id.strip().decode('utf-8') for node_task_id in node_task_id_list]

      print("Task IDs: ", node_list_string)
      all_results = []
      sequence_dict = {}

      for task_id in node_list_string:
        q = Queue('default')
        task = q.fetch_job(task_id)

        if task is not None:
          task_results = task.result["output"]["classification"]
          sequence_ID = task.result["sequence_ID"]
          sequence_dict[sequence_ID] = task_results

      ordered_sequence_dict = collections.OrderedDict(sorted(sequence_dict.items()))
      for k, v in ordered_sequence_dict.items():
        all_results += v

      response = { 
            'result': all_results,
            'unique_id': unique_id,
            'done_task_count': done_task_count.strip().decode('utf-8'),
            'node_task_id_list': node_list_string 
      }

      toc = time.clock()
      job.meta['progress'] = toc - tic
      job.save_meta()

      # Log execution time info to redis
      metas.add_exec_time_info(unique_id, "aggregation", aggregation_start_time, redis_tools.get_redis_server_time())
      print('Output:', response)
      #TODO: Can i instantiate this in worker.py and call it here?
      client = mqtt.Client("SB")
      client.connect(config.MQTT['host'], port=config.MQTT['port'], keepalive=60, bind_address="")

      r_json = json.dumps(response)
      client.publish('hello', payload=r_json)

      return response
