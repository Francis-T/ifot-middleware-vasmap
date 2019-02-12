# Definitions used by the services and their API
import os

NODE_COUNT      = '_node_count'
DONE_NODE_COUNT = '_done_node_count'
TASK_COUNT      = '_task_count'
DONE_TASK_COUNT = '_done_task_count'
EXEC_TIME_INFO  = 'exec_time_info'
TASK_SUFFIX     = '_task'

NANO_SEC_ADJUSTMENT = 1000000000
EXPECTED_TIME_RANGE = NANO_SEC_ADJUSTMENT ** 2

MQTT_HOST           = os.environ['MQTT_HOST']
MQTT_PORT           = int(os.environ['MQTT_PORT'])

INFLUX_HOST           = os.environ['INFLUX_HOST']
INFLUX_PORT           = int(os.environ['INFLUX_PORT'])

REDIS_HOST            = os.environ['REDIS_HOST']
REDIS_PORT            = int(os.environ['REDIS_PORT'])
REDIS_URL             = 'redis://{}:{}/0'.format(REDIS_HOST, REDIS_PORT)
R_TASKS_LIST          = "{}.tasks"
R_TASK_INFO           = "{}.task.{}"
R_TASK_TYPE_ASSIGNED  = "{}.task_type.{}.total_count"
R_TASK_TYPE_TOTAL     = "{}.task_type.{}.total_count"
R_TASK_TYPE_DONE      = "{}.task_type.{}.done_count"


