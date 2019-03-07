import os
import json
import importlib

from flask import Flask
from flask_bootstrap import Bootstrap
from flask_socketio import SocketIO
from flask_cors import CORS
from flask_wtf.csrf import CSRFProtect
from flask_mqtt import Mqtt

import eventlet

eventlet.monkey_patch()

bootstrap = Bootstrap()
socketio = SocketIO()
mqtt = Mqtt()

def create_app(script_info=None):
  app = Flask(
      __name__,
      template_folder='../client/templates',
      static_folder='../client/static'
  )

  app_settings = os.getenv('APP_SETTINGS')
  app.config.from_object(app_settings)

  bootstrap.init_app(app)
  #socketio.init_app(app, async_mode='eventlet', message_queue='redis://')
  socketio.init_app(app, async_mode='eventlet')
  mqtt.init_app(app)
  CORS(app)
  csrf = CSRFProtect(app)

  # Register default front-end Flask Blueprint
  from project.server.main.views import main_blueprint
  app.register_blueprint(main_blueprint, url_prefix='/')

  # Register default API Flask Blueprint
  from project.server.api.views import api_blueprint
  app.register_blueprint(api_blueprint, url_prefix='/api')

  # Dynamically register Flask front-end and API blueprints for services
  service_list = []
  with open('project/server/services.json') as services_file:
    try:
      service_list = json.load(services_file)

    except Exception as e:
      print("ERROR: Failed to load services file")
      print(e)

  for service in service_list:
    try:
      # Register front-end blueprint
      res = importlib.import_module(service['front_end']['path'])
      app.register_blueprint(res.main_blueprint, url_prefix=service['front_end']['url_pref'])

      # Register API blueprint
      res = importlib.import_module(service['api']['path'])
      app.register_blueprint(res.api_blueprint, url_prefix=service['api']['url_pref'])

      print("Loaded: {}".format(service))
    
    except Exception as e:
      print("ERROR: Failed to load {} service".format(service['name']))
      print(e)

  #app.shell_context_processor({'app': app})

  return app

