import json
import paho.mqtt.client as mqtt

from .defs import *

class MqttLog():
  def __init__(self, origin_id, task_id, tags=['default'], host=MQTT_HOST, port=MQTT_PORT):
    self.origin_id = origin_id
    self.task_id = task_id
    self.tag_list = tags
    self.host = host
    self.port = port
    self.client = None

    self.tag_list.append(str(origin_id))
    self.tag_list.append(str(task_id))

    return

  def get_client(self):
    if self.client == None:
      self.client = mqtt.Client(str(self.origin_id))
      print("Connecting to {}:{}".format(self.host, self.port))
      self.client.connect(self.host, port=self.port)

    return self.client

  def msg(self, msg, subtopic=None):
    client = self.get_client()

    print("Publishing: {}".format(msg))
    for t in self.tag_list:
      topic = t
      if subtopic != None:
          topic = "{}/{}".format(t, subtopic)

      mqtt_msg = client.publish(topic, payload=msg, qos=0)
      mqtt_msg.wait_for_publish()
      print("Published result on {}".format(topic))

    return

  def event(self, event_type, status):
    event_data = {
      'origin' : self.origin_id,
      'unique_id' : self.task_id,
      'type'  : 'event',
      'event' : event_type,
      'status' : status,
    }

    return self.msg(json.dumps(event_data))

  def exec_time(self, event_type, start, end):
    event_data = {
      'origin' : self.origin_id,
      'unique_id' : self.task_id,
      'type'  : 'exec_time',
      'event' : event_type,
      'start' : start,
      'end'   : end,
    }

    return self.msg(json.dumps(event_data))

  def results(self, results, subtype=None, metas=None):
      result_data = {
        'origin' : self.origin_id,
        'unique_id' : self.task_id,
        'type'  : 'result',
        'results' : results,
      }

      if subtype != None:
          result_data['subtype'] = subtype

      if metas != None:
        result_data['metas'] = metas

      return self.msg(json.dumps(result_data), subtopic='results')

