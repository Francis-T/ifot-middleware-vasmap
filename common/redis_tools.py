import redis
import pandas

def setRedisKV(r, K, V):
  try:
    r.set(K, V)
    return True
  except:
    return False

def getRedisV(r, K):
  output = r.get(K)
  if output is not None:
    return output
  else:
    return "Empty"

def incrRedisKV(r, K):
  try:
    r.incr(K)
    return True
  except:
    return False


def getListK(r, K):
  output = r.lrange(K, 0, -1)
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

#Probably should have redis as input param
def get_redis_server_time():
  # Start a redis connection
  r = redis.StrictRedis(host="redis", port=6380, password="", decode_responses=True)
  sec, microsec = r.time()
  return ((sec * 1000000) + microsec)

def store_dataframe_with_key(redis, key, dataframe):
  redis.set(key, dataframe.to_msgpack(compress='zlib'))
  return key
