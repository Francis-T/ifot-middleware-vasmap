import redis
import datetime
import requests
from dateutil import tz

from .defs import *
from .redis_tools import *

#TODO: Probably should have redis connection as input param
def initialize_query(total, count_suffix=NODE_COUNT, done_count_suffix=DONE_NODE_COUNT, use_count=True):
  r = redis.StrictRedis(host=REDIS_HOST, port=REDIS_PORT, password="", decode_responses=True)

  unique_id = generate_unique_ID()
  setRedisKV(r, unique_id, 'ongoing')

  if use_count:
      setRedisKV(r, unique_id + count_suffix, total)
      setRedisKV(r, unique_id + done_count_suffix, 0)

  return unique_id

#TODO: Influx db client as input param
def query_influx_db(start, end, fields="*",
                                influx_db='IFoT-GW2',
                                influx_ret_policy='autogen',
                                influx_meas='IFoT-GW2-Meas',
                                host='163.221.68.191',
                                port='8086'):

    # Build the filter clause
    where = ""
    if start < EXPECTED_TIME_RANGE:
      start = int(start) * NANO_SEC_ADJUSTMENT

    if end < EXPECTED_TIME_RANGE:
      end = int(end) * NANO_SEC_ADJUSTMENT

    source = '"{}"."{}"."{}"'.format(influx_db, influx_ret_policy, influx_meas)
    where  = 'WHERE time >= {} AND time <= {}'.format(start, end)
    query = "SELECT {} from {} {}".format(fields, source, where)

    payload = {
        "db": influx_db,
        "pretty": True,
        "epoch": 'ms',
        "q": query
    }

    influx_url = "http://{}:{}/query".format(host, port)
    return requests.get(influx_url, params=payload)

def generate_unique_ID():
  return datetime.datetime.now().strftime('%Y%m%d%H%M%S%f')
  
def get_current_time():
  HERE = tz.gettz('Asia/Tokyo')
  UTC = tz.gettz('UTC')

  ts = datetime.datetime.utcnow().replace(tzinfo=UTC).astimezone(HERE)
  # local_time = ts.strftime('%Y-%m-%d %H:%M:%S.%f %Z%z')
  local_time = ts.strftime('%Y-%m-%d %H:%M:%S.%f %Z')[:-3]
  return local_time
