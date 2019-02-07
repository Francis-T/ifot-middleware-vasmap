import redis
import config
import json

from rq import Worker, Connection, get_current_job

from common.defs import *

def main():
    redis_connection = redis.from_url(REDIS_URL) 
    task_queues = ['default']

    with open("./data/node_info.json") as node_info_file:
        # Load node identity from "./data/node_info.json"
        node_info = json.load(node_info_file)

        # Add node's own identity to the task queues to listen on
        task_queues.append( node_info['node_id'])

    # Connect to the redis queue and monitor
    with Connection(redis_connection):
        worker = Worker(task_queues)
        worker.work()

if __name__ == '__main__':
    main()


