from flask import render_template, Blueprint, url_for, redirect, request, jsonify
from werkzeug.utils import secure_filename

import requests
import eventlet
import json
import numpy as np
import random
import math
import ast
import datetime
import os

from rq import Connection
from influxdb import InfluxDBClient

from .. import socketio
from ..tools.utils import blockshaped, get_nrows, is_valid, get_64_node_json, delete_db, create_db, id_generator, random_speed
from ..forms.vas_rsu_form import VasRSUForm, VasPopulate, VasDelayProfileForm, VasAddRsuForm, VasGenerateRsuForm, VasGenerateRsuDataForm
from ...common.defs import *
from ...common import general_tools
from ..main import views

eventlet.monkey_patch()

main_blueprint = Blueprint('vas', __name__,)

ALLOWED_EXTENSIONS = [ 'json' ]

###
## S0001  Vehicle Average Speed Service Routes
###
@main_blueprint.route('/vas_sysconfig', methods=['GET', 'POST'])
def vas_sysconfig():
  add_rsu_form = VasAddRsuForm()
  generate_rsu_form = VasGenerateRsuForm()
  generate_rsu_data_form = VasGenerateRsuDataForm()
  rsu_list_url = "http://localhost:5011/api/vas/request_rsu_list"
  try:
    r = requests.get(rsu_list_url)
  except requests.ConnectionError:
    return "Connection Error"  + rsu_list_url
  rsu_list = json.loads(r.text)
  mid = int(len(rsu_list) / 2)

  rsu_data_counts_url = "http://localhost:5011/api/vas/request_data_counts"
  try:
    r = requests.get(rsu_data_counts_url)
  except requests.ConnectionError:
    return "Connection Error"  + rsu_data_counts_url
  rsu_data_counts = json.loads(r.text)

  for rsu in rsu_list:
    if rsu['rsu_id'] in rsu_data_counts:
      data_count = rsu_data_counts[rsu['rsu_id']]
      rsu['data_count'] = data_count
    else:
      rsu['data_count'] = 0

  return render_template('vas_sysconfig.html', add_rsu_form=add_rsu_form,
                                               generate_rsu_form=generate_rsu_form,
                                               generate_rsu_data_form=generate_rsu_data_form,
                                               rsu_lists=[ rsu_list[:mid], rsu_list[mid:] ] )

@main_blueprint.route('/upload_rsu_file', methods=['POST'])
def upload_rsu_file():
  f = request.files['rsu_file']
  rsu_data = json.loads(f.read())
  load_rsu_file(rsu_data)

  return redirect(url_for('vas.vas_sysconfig'))

@main_blueprint.route('/generate_rsu_speed_data', methods=['POST'])
def vas_generate_speed_data():
  target_rsu_ids = json.loads(request.form['generate_data_rsu_list'])
  data_count = int(request.form['data_count'])

  # Get the RSU list and filter out the non-target RSUs
  rsu_list_url = "http://localhost:5011/api/vas/request_rsu_list"
  try:
    r = requests.get(rsu_list_url)
  except requests.ConnectionError:
    return "Connection Error"  + rsu_list_url
  initial_rsu_list = json.loads(r.text)

  if len(initial_rsu_list) <= 0:
    return redirect(url_for('vas.vas_sysconfig'))

  target_rsu_list = []
  for rsu in initial_rsu_list:
    if rsu['rsu_id'] in target_rsu_ids:
      target_rsu_list.append(rsu)

  initial_rsu_list = None

  # Request data generation
  result = generate_rsu_speed_data(target_rsu_list, data_count)
  print(result)

  return redirect(url_for('vas.vas_sysconfig'))

@main_blueprint.route('/clear_rsu_speed_data', methods=['POST'])
def vas_clear_rsu_speed_data():
  client = InfluxDBClient('influxdb', 8086)

  # Delete and recreate the database
  delete_db(client, 'rsu_speed')
  create_db(client, 'rsu_speed')

  return redirect(url_for('vas.vas_sysconfig'))

@main_blueprint.route('/clear_rsu_data', methods=['POST'])
def vas_clear_rsu_data():
  client = InfluxDBClient('influxdb', 8086)

  # Delete and recreate the database
  delete_db(client, 'rsu_id_location')
  create_db(client, 'rsu_id_location')

  return redirect(url_for('vas.vas_sysconfig'))


@main_blueprint.route('/vas_setup', methods=['GET', 'POST'])
def vas_setup():
  #rsu_list_url = request.url_root + 'api/vas/request_rsu_list'
  form = VasRSUForm(request.form)
  populate = VasPopulate()
  delay_profile_form = VasDelayProfileForm()

  rsu_list_url = "http://localhost:5011/api/vas/request_rsu_list"
  if request.method == 'POST' and 'setup' in request.form:
    print('Form errors:', form.errors)
    number_of_workers = int(request.form['number_of_workers'])
    number_of_masters = int(request.form['number_of_masters'])
 
    try:
      r = requests.get(rsu_list_url)
    except requests.ConnectionError:
      return "Connection Error"  + rsu_list_url
    rsu_list = json.loads(r.text)
    rsu_count = len(rsu_list)

    #if not is_valid(number_of_workers, number_of_masters, rsu_count):
    if ( (number_of_workers * number_of_masters) > rsu_count ):
      return redirect(url_for('vas.vas_setup'))

    cluster_data = generate_random_clusters(rsu_list, (number_of_workers - 1), number_of_masters)
    # return jsonify(out_list)
    #return "Hello world!" + str(number_of_workers) + ',' + str(number_of_masters) + ',' + str(rsu_count)

    possible_workers = [64, 32, 16, 8, 2, 1]
    possible_masters = [1, 2, 4, 8, 16, 32]

    form.number_of_workers.choices = [ (str(x), str(x)) for x in possible_workers ]
    form.number_of_masters.choices = [ (str(x), str(x)) for x in possible_masters ]

    return render_template('vas_setup.html', form=form,
                                             populate=populate,
                                             dp_form=delay_profile_form,
                                             rsu_count=len(rsu_list),
                                             master_rsu_list=cluster_data['full_rsu_list'],
                                             json_rsu_list=json.dumps(cluster_data['json_rsu_list']))

  # elif request.method == 'GET':
  try:
    r = requests.get(rsu_list_url)
  except requests.ConnectionError:
    return "Connection Error"  + rsu_list_url

  rsu_list = json.loads(r.text)
  rsu_count = len(rsu_list)
  possible_workers = [64, 32, 16, 8, 2, 1]
  possible_masters = [1, 2, 4, 8, 16, 32]

  form.number_of_workers.choices = [ (str(x), str(x)) for x in possible_workers ]
  form.number_of_masters.choices = [ (str(x), str(x)) for x in possible_masters ]

  return render_template('vas_setup.html', form=form, 
                                           populate=populate,
                                           dp_form=delay_profile_form,
                                           rsu_count=len(rsu_list))

@main_blueprint.route('/vas_simulate', methods=['GET', 'POST'])
def vas_simulate():
  if request.method == 'GET':
    return render_template("vas_simulate.html", error_msg="Request Type Error: Input parameters not found")

  if not 'rsu_cluster_data' in request.form:
    return render_template("vas_simulate.html", error_msg="Parameter Error: No RSU cluster data")

  rsu_list_url = 'http://localhost:5011/api/vas/request_rsu_list'

  resp = []
  try:
    resp = requests.get(rsu_list_url)
  except requests.ConnectionError:
    error_msg = "Connection Error: Could not retrieve RSU list from {}".format(rsu_list_url)
    return render_template("vas_simulate.html", error_msg=error_msg)

  raw_cluster_data = json.loads(request.form['rsu_cluster_data'])
  cluster_data = {}
  for sub_list in raw_cluster_data:
    for master_rsu, cluster_rsu_list in sub_list.items():
      cluster_data[master_rsu] = cluster_rsu_list

  rsu_list = json.loads(resp.text)

  filtered_rsu_list = []
  for rsu in rsu_list:
    is_included = False
    # Look for the RSU id in our cluster data
    for master_rsu in list(cluster_data.keys()):
      if rsu['rsu_id'] == master_rsu:
        is_included = True
        break

      if rsu['rsu_id'] in cluster_data[master_rsu]:
        is_included = True
        break

    if is_included:
      filtered_rsu_list.append(rsu)

  svc_params = {
      "host": INFLUX_HOST,
      "port": INFLUX_PORT,
      "db_name": "rsu_speed",
      "db_ret_policy": "autogen",
      "db_meas": "rsu_speeds",
      "start_time": 0,
      "end_time": 1549086979,
  } 

  delay_profile = {
    'gateway_tx_rate'        : float(request.form['gateway_tx_rate']),
    'cluster_tx_rate'        : float(request.form['cluster_tx_rate']),
    'gateway_prop_speed'     : float(request.form['gateway_prop_speed']),
    'cluster_prop_speed'     : float(request.form['cluster_prop_speed']),
    'gateway_link_length'    : float(request.form['gateway_link_length']),
    'cluster_link_length'    : float(request.form['cluster_link_length']),
    'gateway_proc_delay'     : float(request.form['gateway_proc_delay']),
    'cluster_proc_delay'     : float(request.form['cluster_proc_delay']),
    'gateway_queueing_delay' : float(request.form['gateway_queueing_delay']),
    'cluster_queueing_delay' : float(request.form['cluster_queueing_delay']),
  }

  return render_template("vas_simulate.html", rsu_list=json.dumps(filtered_rsu_list), 
                                              cluster_data=json.dumps(cluster_data), 
                                              svc_params=json.dumps(svc_params),
                                              delay_profile=json.dumps(delay_profile),
                                              strategy=json.dumps(request.form['strategy']))

@main_blueprint.route('/get_rsu_list', methods=['GET'])
def get_rsu_list():
  rsu_list_url = "http://localhost:5011/api/vas/request_rsu_list"
  try:
    r = requests.get(rsu_list_url)
  except requests.ConnectionError:
    return "Connection Error"  + rsu_list_url
  rsu_list = json.loads(r.text)
  return jsonify(rsu_list)

def load_rsu_file(rsu_list):
  client = InfluxDBClient('influxdb', 8086)

  # Delete and recreate the database
  delete_db(client, 'rsu_id_location')
  create_db(client, 'rsu_id_location')

  client.switch_database('rsu_id_location')
  date = datetime.datetime(2018,12,1,12,0,0)

  influx_rsu_list = []
  for rsu in rsu_list:
    date += datetime.timedelta(days=1)
    dateStr = date.strftime("%Y-%m-%dT%H:%M:%S") + 'Z'
    data = {
        'time' : dateStr,
        'measurement' : 'rsu_locations',
        'tags' : {
            'host' : INFLUX_HOST,
        },
        'fields' : {
            'rsu-id' : rsu['rsu_id'],
            'lat'    : rsu['lat'],
            'lon'    : rsu['lon'],
        }
    }

    influx_rsu_list.append(data)

  client.write_points(influx_rsu_list)

  return

def create_rsu_data_fom_count(rsu_count, bounds):
  client = InfluxDBClient('influxdb', 8086)

  # Delete and recreate the database
  delete_db(client, 'rsu_id_location')
  create_db(client, 'rsu_id_location')

  client.switch_database('rsu_id_location')
  date = datetime.datetime(2018,12,1,12,0,0)

  edge_rsu_count = int(math.sqrt(rsu_count / 2))

  # Divide the bounds into appropriate lat and lon steps
  lon_step = abs( bounds['lon'][0] - bounds['lon'][1] ) / float(edge_rsu_count)
  lat_step = abs( bounds['lat'][0] - bounds['lat'][1] ) / float(edge_rsu_count)

  influx_rsu_list = []
  for y in edge_rsu_count:
    for x in edge_rsu_count:
      date += datetime.timedelta(days=1)
      dateStr = date.strftime("%Y-%m-%dT%H:%M:%S") + 'Z'
      data = {
          'time' : dateStr,
          'measurement' : 'rsu_locations',
          'tags' : {
              'host' : INFLUX_HOST,
          },
          'fields' : {
              'rsu-id' : rsu['rsu_id'],
              'lat'    : (min( bounds['lat'] ) + (y * lat_step)),
              'lon'    : (min( bounds['lon'] ) + (x * lon_step)),
          }
      }

      influx_rsu_list.append(data)

  client.write_points(influx_rsu_list)

  return

def generate_rsu_speed_data(target_rsu_list, rows_of_data):
  client = InfluxDBClient('influxdb', 8086)

  # Switch to the speed daatabase
  create_db(client, 'rsu_speed')
  client.switch_database('rsu_speed')

  date = datetime.datetime(2016,12,1,12,0,0)
  influx_node_list = []
  for i in range(rows_of_data):
    for rsu in target_rsu_list:
      fields = {}
      tags = {}
      data = {}
      speed = round(random_speed(44, 77), 2)
      car_id = id_generator(6)
      dateStr = date.strftime("%Y-%m-%dT%H:%M:%S") + 'Z'

      data = {
        'time' : dateStr,
        'measurement' : 'rsu_speeds',
        'tags' : {
            'node' :    rsu['rsu_id'],
        },
        'fields' : {
          'rsu_id' :    rsu['rsu_id'],
          'lat' :       str(rsu['lat']),
          'lng' :       str(rsu['lon']),
          'speed' :     speed,
          'car_id' :    car_id,
          'direction' : 'n.bound',
        },
      }

      influx_node_list.append(data)
      date += datetime.timedelta(seconds=1)

  client.write_points(influx_node_list)

  result = client.query('select count(*) from "rsu_speed"."autogen"."rsu_speeds";')
  points = result.get_points()
  temp = {}
  for item in points:
    temp = item

  total_rows = temp['count_car_id']
  return "Generated {} of data across 64 nodes".format(str(total_rows))

def generate_random_clusters(rsu_list, cluster_node_count, cluster_count):
  # Copy the original rsu list
  working_rsu_list = [ rsu['rsu_id'] for rsu in rsu_list ] 
  rsu_count = len(working_rsu_list)

  # Choose {cluster_count} cluster heads and remove them from the list
  cluster_head_list = random.sample(working_rsu_list, cluster_count)
  [ working_rsu_list.remove(rsu) for rsu in cluster_head_list ]

  # TODO Assign up to {(node_count / cluster_count) + 1} nodes to each cluster head
  out_list = []
  # cluster_node_count = int((rsu_count - len(cluster_head_list)) / cluster_count) 
  # if cluster_node_count > node_count:
  #     cluster_node_count = node_count - 1

  for cluster_head in cluster_head_list:
    cluster_nodes = []
    if len(working_rsu_list) > cluster_node_count:
      print("Case 1")
      cluster_nodes = random.sample(working_rsu_list, cluster_node_count)

    else:
      print("Case 2")
      cluster_nodes = working_rsu_list.copy()

    t_dict = {}
    t_dict[cluster_head] = cluster_nodes
    out_list.append(t_dict)

    # Remove previously added nodes
    [ working_rsu_list.remove(rsu) for rsu in cluster_nodes ]
    print("Working RSU List ({}): {}".format(len(working_rsu_list), working_rsu_list))


  # TODO Format and return the result
  # Create a full RSU list with master RSU and location information
  full_rsu_list = []
  for cluster in out_list:
    for master in list(cluster.keys()):
      temp = cluster[master]
      temp_rsu_list = []
      for target_rsu_id in temp:
        for rsu in rsu_list:
          if rsu['rsu_id'] == target_rsu_id:
            temp_rsu_list.append(rsu)
            break

      t_dict = {}
      t_dict[master] = temp_rsu_list
      full_rsu_list.append(t_dict)

  return { 'full_rsu_list' : full_rsu_list, 'json_rsu_list' : out_list }

def generate_localized_clusters(rsu_list, cluster_node_count, cluster_count):
  rsu_count = len(rsu_list)
  rsus = [rsu for rsu in range(0, rsu_count)]
  rsus_np = np.asarray(rsus)

  B = np.reshape(rsus_np, (-1, math.floor(math.sqrt(rsu_count))))
  C = B.T
  D = np.flip(C, 0)

  nrows = get_nrows(cluster_node_count)
  sets_of_rsus = blockshaped(D, nrows, (cluster_node_count + 1) // nrows)

  out_list = []
  for set_of_rsu in sets_of_rsus[0:cluster_count]:
    t_dict = {}
    temp = ['RSU-' + str(rsu).zfill(4) for rsu in set_of_rsu.flatten().tolist()]
    master = random.choice(temp)
    temp.remove(master)
    t_dict[master] = temp
    out_list.append(t_dict)

  # Create a full RSU list with master RSU and location information
  full_rsu_list = []
  for set_of_rsu in sets_of_rsus[0:cluster_count]:
    t_dict = {}
    temp = ['RSU-' + str(rsu).zfill(4) for rsu in set_of_rsu.flatten().tolist()]
    master = random.choice(temp)
    temp.remove(master)

    temp_rsu_list = []
    for target_rsu_id in temp:
      for rsu in rsu_list:
        if rsu['rsu_id'] == target_rsu_id:
          temp_rsu_list.append(rsu)
          break

    t_dict[master] = temp_rsu_list
    full_rsu_list.append(t_dict)

  return { 'full_rsu_list' : full_rsu_list, 'json_rsu_list' : out_list }

def allowed_file(filename):
  return '.' in filename and \
         filename.rsplit('.', 1)[1].lower() in ALLOWED_EXTENSIONS

@socketio.on('function')
def log_message(message):
  function = message['data']
  number_of_rows = message['number_of_rows']
  print('received: ' + str(function))
  response = vas_populate(int(number_of_rows))
  socketio.emit('status', {'data': response})

