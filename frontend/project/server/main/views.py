from flask import Flask, render_template, Blueprint, current_app
from flask import url_for, redirect, send_from_directory, request
from flask import jsonify
import requests
import os
import eventlet
from .. import socketio, mqtt
from ..tools.utils import blockshaped, get_nrows, is_valid, vas_populate
from ..forms.upload_form import UploadForm
from ..forms.vas_rsu_form import VasRSUForm, VasPopulate
from ...common.defs import *
from ...common import general_tools
from werkzeug.utils import secure_filename
import redis
from rq import Queue, Connection
import json
import numpy as np
import random
import math

eventlet.monkey_patch()

main_blueprint = Blueprint('main', __name__,)

@main_blueprint.route('/', methods=['GET'])
def index():
  return render_template('index.html')

@main_blueprint.route('/upload', methods=['GET', 'POST'])
def upload():
  form = UploadForm()
  if form.validate_on_submit():
    file = form.file.data
    if file and general_tools.allowed_file(file.filename):
      filename = secure_filename(file.filename)
      os.makedirs(os.path.join(current_app.instance_path, 'htmlfi'), exist_ok=True)
      file.save(os.path.join(current_app.instance_path, 'htmlfi', filename))
      return redirect(url_for('main.uploaded_file', filename=filename))
  return render_template('upload.html', form=form, result=None)

@main_blueprint.route('/uploads/<filename>')
def uploaded_file(filename):
  return send_from_directory(os.path.join(current_app.instance_path, 'htmlfi'), filename)

@main_blueprint.route('/vas_setup', methods=['GET', 'POST'])
def vas_setup():
  rsu_list_url = request.url_root + 'api/vas/request_rsu_list'
  if request.method == 'POST' and 'setup' in request.form:
    form = VasRSUForm(request.form)
    populate = VasPopulate()
    print('Form errors:', form.errors)
    number_of_workers = int(request.form['number_of_workers'])
    number_of_masters = int(request.form['number_of_masters'])
 
    try:
      r = requests.get(rsu_list_url)
    except requests.ConnectionError:
      return "Connection Error"  + rsu_list_url
    rsu_list = json.loads(r.text)
    rsu_count = len(rsu_list)

    if not is_valid(number_of_workers, number_of_masters, rsu_count):
      return redirect(url_for('main.vas_setup'))
    rsus = [rsu for rsu in range(0, rsu_count)]
    rsus_np = np.asarray(rsus)

    B = np.reshape(rsus_np, (-1, math.floor(math.sqrt(rsu_count))))
    C = B.T
    D = np.flip(C, 0)

    nrows = get_nrows(number_of_workers)
    sets_of_rsus = blockshaped(D, nrows, (number_of_workers + 1)//nrows)

    out_list = []
    for set_of_rsu in sets_of_rsus[0:number_of_masters]:
      t_dict = {}
      temp = ['RSU-' + str(rsu).zfill(4) for rsu in set_of_rsu.flatten().tolist()]
      master = random.choice(temp)
      temp.remove(master)
      t_dict[master] = temp
      out_list.append(t_dict)

    # Create a full RSU list with master RSU and location information
    full_rsu_list = []
    for set_of_rsu in sets_of_rsus[0:number_of_masters]:
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

    # return jsonify(out_list)
    #return "Hello world!" + str(number_of_workers) + ',' + str(number_of_masters) + ',' + str(rsu_count)

    possible_workers = [63, 31, 15, 7, 3, 1]
    possible_masters = [1, 2, 4, 8, 16, 32]

    form.number_of_workers.choices = [ (str(x), str(x)) for x in possible_workers ]
    form.number_of_masters.choices = [ (str(x), str(x)) for x in possible_masters ]

    return render_template('vas_setup.html', form=form,
                                             populate=populate,
                                             rsu_count=len(rsu_list),
                                             master_rsu_list=full_rsu_list,
                                             json_rsu_list=json.dumps(out_list))

  # elif request.method == 'GET':
  form = VasRSUForm()
  populate = VasPopulate()
  try:
    r = requests.get(rsu_list_url)
  except requests.ConnectionError:
    return "Connection Error"  + rsu_list_url

  print(r.text)
  rsu_list = json.loads(r.text)
  rsu_count = len(rsu_list)
  possible_workers = [63, 31, 15, 7, 3, 1]
  possible_masters = [1, 2, 4, 8, 16, 32]

  form.number_of_workers.choices = [ (str(x), str(x)) for x in possible_workers ]
  form.number_of_masters.choices = [ (str(x), str(x)) for x in possible_masters ]

  return render_template('vas_setup.html', form=form, populate=populate, rsu_count=len(rsu_list))

@main_blueprint.route('/vas_simulate', methods=['GET', 'POST'])
def vas_simulate():
  if request.method == 'GET':
    return render_template("vas_simulate.html", error_msg="Request Type Error: Input parameters not found")

  if not 'rsu_cluster_data' in request.form:
    return render_template("vas_simulate.html", error_msg="Parameter Error: No RSU cluster data")

  rsu_list_url = request.url_root + 'api/vas/request_rsu_list'

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

  return render_template("vas_simulate.html", rsu_list=json.dumps(filtered_rsu_list), 
                                              cluster_data=json.dumps(cluster_data), 
                                              svc_params=json.dumps(svc_params) )

@main_blueprint.route('/iris_classifier', methods=['GET', 'POST'])
def iris_classifier():
  form = UploadForm()
  if request.method == 'POST':
    if form.validate_on_submit():
      file = form.file.data
      if file and general_tools.allowed_file(file.filename):
        filename = secure_filename(file.filename)
        os.makedirs(os.path.join(current_app.instance_path, 'htmlfi'), exist_ok=True)
        file.save(os.path.join(current_app.instance_path, 'htmlfi', filename))
        print(filename)
        url = request.url_root + 'api/iris_dist_process'
        data = {"filename": filename,
                "nodes": 1}
        print(url, data)
        r = requests.post(url, json=data)
        dictFromServer = r.json()
        return redirect(url_for('main.monitor'))
        # return 'received response from API: ' + dictFromServer['response']
  elif request.method == 'GET':
      return render_template('upload.html', form=form, result=None)

@main_blueprint.route('/monitor', methods=['GET'])
def monitor():
  mqtt.subscribe('hello')
  return render_template('monitor.html')

@socketio.on('subscribe')
def subscribe(message):
  topic = message['topic']
  mqtt.subscribe(topic)
  print("Subscribed to: {}".format(topic))
  return

@socketio.on('my event')
def log_message(message):
  # socketio.emit('my response', {'data': 'got it!'})
  print('received: ' + str(message))

@socketio.on('function')
def log_message(message):
  function = message['data']
  number_of_rows = message['number_of_rows']
  print('received: ' + str(function))
  response = vas_populate(int(number_of_rows))
  socketio.emit('status', {'data': response})

@mqtt.on_message()
def handle_mqtt_message(client, userdata, message):
  data = dict(
      topic=message.topic,
      payload=message.payload.decode()
  )
  print(data)
  monitor = dict(
    user_name=data['topic'],
    message=data['payload']
  )

  socketio.emit('monitor message', data=monitor)

  if "results" in data['topic']:
    socketio.emit('results', data['payload'])

  return

@mqtt.on_log()
def handle_logging(client, userdata, level, buf):
    print(level, buf)

