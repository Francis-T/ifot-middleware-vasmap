import redis
import datetime
from dateutil import tz
from .Defs import *
import json
import sys
sys.path.append("..")
import config

def delRedisFromAllKeys(r, K):
  try:
    for key in K:
      r.delete(K)
  except:
    return false

def setRedisKV(r, K, V):
  try:
    r.set(K, V)
    return True
  except:
    return False

def incrRedisKV(r, K):
  try:
    r.incr(K)
    return True
  except:
    return False

def getRedisV(r, K):
  output = r.get(K)
  if output is not None:
    return output
  else:
    return "Empty"

def appendToListK(r, K, V):
  try:
    r.rpush(K, V)
    return True
  except:
    return False

def getListK(r, K):
  output = r.lrange(K, 0, -1)
  if output is not None:
    return output
  else:
    return "Empty"

def get_current_time():
  HERE = tz.gettz('Asia/Tokyo')
  UTC = tz.gettz('UTC')

  ts = datetime.datetime.utcnow().replace(tzinfo=UTC).astimezone(HERE)
  # local_time = ts.strftime('%Y-%m-%d %H:%M:%S.%f %Z%z')
  local_time = ts.strftime('%Y-%m-%d %H:%M:%S.%f %Z')
  return local_time

def get_redis_server_time():
  # Start a redis connection
  r = redis.StrictRedis(host=config.REDIS['host'], port=config.REDIS['port'], password="", decode_responses=True)
  sec, microsec = r.time()
  return ((sec * 1000000) + microsec)

def add_exec_time_info(unique_id, operation, time_start, time_end):
  r = redis.StrictRedis(host=config.REDIS['host'], port=config.REDIS['port'], password="", decode_responses=True)

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

