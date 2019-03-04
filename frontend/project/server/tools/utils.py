import numpy as np
import random
import string
from influxdb import InfluxDBClient
import ast
import datetime
import json
from ...common.defs import *

def blockshaped(arr, nrows, ncols):
    """
    Return an array of shape (n, nrows, ncols) where
    n * nrows * ncols = arr.size

    If arr is a 2D array, the returned array should look like n subblocks with
    each subblock preserving the "physical" layout of arr.
    """
    h, w = arr.shape
    print(h//nrows)
    print(nrows, ncols)
    return (arr.reshape(h//nrows, nrows, -1, ncols)
               .swapaxes(1,2)
               .reshape(-1, nrows, ncols))

def get_nrows(no_of_workers):
    return {
      '63': 8,
      '31': 4,
      '15': 2,
      '7' : 2,
      '3' : 4,
      '1' : 1
    }.get(no_of_workers, 4)

def is_valid(workers, masters, rsu_count):
    p = (workers + 1) * masters
    if p > rsu_count:
        return False
    elif workers == 63 and masters == 1:
        return False
    return True

def get_64_node_json():
    return '[["RSU-0000", "-87.07", "35.958"], ["RSU-0001", "-87.07", "36.030415"], ["RSU-0002", "-87.07", "36.102833"], ["RSU-0003", "-87.07", "36.175247"], ["RSU-0004", "-87.07", "36.247665"], ["RSU-0005", "-87.07", "36.32008"], ["RSU-0006", "-87.07", "36.392498"], ["RSU-0007", "-87.07", "36.464912"], ["RSU-0008", "-86.98015", "35.958"], ["RSU-0009", "-86.98015", "36.030415"], ["RSU-0010", "-86.98015", "36.102833"], ["RSU-0011", "-86.98015", "36.175247"], ["RSU-0012", "-86.98015", "36.247665"], ["RSU-0013", "-86.98015", "36.32008"], ["RSU-0014", "-86.98015", "36.392498"], ["RSU-0015", "-86.98015", "36.464912"], ["RSU-0016", "-86.89029", "35.958"], ["RSU-0017", "-86.89029", "36.030415"], ["RSU-0018", "-86.89029", "36.102833"], ["RSU-0019", "-86.89029", "36.175247"], ["RSU-0020", "-86.89029", "36.247665"], ["RSU-0021", "-86.89029", "36.32008"], ["RSU-0022", "-86.89029", "36.392498"], ["RSU-0023", "-86.89029", "36.464912"], ["RSU-0024", "-86.80044", "35.958"], ["RSU-0025", "-86.80044", "36.030415"], ["RSU-0026", "-86.80044", "36.102833"], ["RSU-0027", "-86.80044", "36.175247"], ["RSU-0028", "-86.80044", "36.247665"], ["RSU-0029", "-86.80044", "36.32008"], ["RSU-0030", "-86.80044", "36.392498"], ["RSU-0031", "-86.80044", "36.464912"], ["RSU-0032", "-86.71058", "35.958"], ["RSU-0033", "-86.71058", "36.030415"], ["RSU-0034", "-86.71058", "36.102833"], ["RSU-0035", "-86.71058", "36.175247"], ["RSU-0036", "-86.71058", "36.247665"], ["RSU-0037", "-86.71058", "36.32008"], ["RSU-0038", "-86.71058", "36.392498"], ["RSU-0039", "-86.71058", "36.464912"], ["RSU-0040", "-86.62073", "35.958"], ["RSU-0041", "-86.62073", "36.030415"], ["RSU-0042", "-86.62073", "36.102833"], ["RSU-0043", "-86.62073", "36.175247"], ["RSU-0044", "-86.62073", "36.247665"], ["RSU-0045", "-86.62073", "36.32008"], ["RSU-0046", "-86.62073", "36.392498"], ["RSU-0047", "-86.62073", "36.464912"], ["RSU-0048", "-86.53087", "35.958"], ["RSU-0049", "-86.53087", "36.030415"], ["RSU-0050", "-86.53087", "36.102833"], ["RSU-0051", "-86.53087", "36.175247"], ["RSU-0052", "-86.53087", "36.247665"], ["RSU-0053", "-86.53087", "36.32008"], ["RSU-0054", "-86.53087", "36.392498"], ["RSU-0055", "-86.53087", "36.464912"], ["RSU-0056", "-86.44102", "35.958"], ["RSU-0057", "-86.44102", "36.030415"], ["RSU-0058", "-86.44102", "36.102833"], ["RSU-0059", "-86.44102", "36.175247"], ["RSU-0060", "-86.44102", "36.247665"], ["RSU-0061", "-86.44102", "36.32008"], ["RSU-0062", "-86.44102", "36.392498"], ["RSU-0063", "-86.44102", "36.464912"]]'

def random_speed(min, max):
    return random.uniform(min, max)

def id_generator(size=6, chars=string.ascii_uppercase + string.digits):
   return ''.join(random.choice(chars) for _ in range(size))

def create_db(client, dbName):
    dbList = client.get_list_database()
    dbArr = []
    for db in dbList:
        dbArr.append(db['name'])

    if dbName in dbArr:
        print("Already exists: " + dbName)
    else:
        client.create_database(dbName)
        print("Created db: " + dbName)

def delete_db(client, dbName):
    dbList = client.get_list_database()
    dbArr = []
    for db in dbList:
        dbArr.append(db['name'])

    if dbName in dbArr:
        print("Already exists: " + dbName)
        client.switch_database(dbName)
        print("Switched to db: " + dbName)
        client.drop_database(dbName)
        print("Dropped db: " + dbName)

