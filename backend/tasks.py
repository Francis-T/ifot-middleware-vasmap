import time
import redis
from rq import get_current_job, Queue, Connection
import socket
import pandas as pd
from sklearn import svm
from sklearn.externals import joblib

from common.defs import *
from common import redis_tools
from common import metas as meta_tools
import config
import json

#TODO: Check if data is not mangled or truncated:
#https://stackoverflow.com/questions/37943778/how-to-set-get-pandas-dataframe-to-from-redis

def classify_iris(unique_ID, sequence_ID, redis_df_key):
  process_start_time = redis_tools.get_redis_server_time()
  job = get_current_job()

  tic = time.clock()
  job.meta['handled_by'] = socket.gethostname()
  job.meta['handled_time'] = int(time.time())
  job.save_meta()
  
  r = redis.StrictRedis(host=config.REDIS['host'],
                                       port=config.REDIS['port'],
                                       password="", decode_responses=False)

  redis_tools.appendToListK(r, unique_ID + TASK_SUFFIX, job.id)

  df = pd.read_msgpack(r.get(redis_df_key))
  print(df.shape)

  clf = joblib.load('models/SVM_iris.pkl')
  prob = clf.predict_proba(df)

  df_prob = pd.DataFrame(prob)
  prob_idx = df_prob.idxmax(axis=1)
  
  feature_list = ['setosa', 'versicolor', 'virginica']

  output = [feature_list[x] for x in prob_idx]
  results = {}
  results['classification'] = output

  toc = time.clock()
  job.meta['progress'] = toc - tic
  job.save_meta()

  redis_tools.incrRedisKV(r, unique_ID + DONE_TASK_COUNT)
  task_count = redis_tools.getRedisV(r, unique_ID + TASK_COUNT).strip().decode('utf-8')
  done_task_count = redis_tools.getRedisV(r, unique_ID + DONE_TASK_COUNT).strip().decode('utf-8')

  metas = {
    'unique_id' : unique_ID,
    'task_count' : task_count,
    'done_task_count' : done_task_count,
    'feature_list' : feature_list,
  }

  if task_count == done_task_count:
    redis_tools.setRedisKV(r, unique_ID, "finished")
    with Connection(r):
      q = Queue('aggregator')
      t = q.enqueue('tasks.aggregate_iris', unique_ID, depends_on=job.id)
      metas['agg_task_id'] = t.id
  else:
    print('still not done processing')

  meta_tools.add_exec_time_info(unique_ID, "processing-{}".format(sequence_ID), process_start_time, redis_tools.get_redis_server_time())

  response = { 'sequence_ID': sequence_ID, 'metas' : metas, 'output': results, 'outsize': len(results)}
  print('Backend: ', response)
  return response
