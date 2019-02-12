import os
basedir = os.path.abspath(os.path.dirname(__file__))

class BaseConfig(object):
    """Base configuration."""
    WTF_CSRF_ENABLED = True
    QUEUES = ['default']
    SQLALCHEMY_DATABASE_URI = os.environ.get('DATABASE_URL') or \
        'sqlite:///' + os.path.join(basedir, 'app.db')
    SQLALCHEMY_TRACK_MODIFICATIONS = False
    UPLOAD_FOLDER = 'client/static/uploads'
    APP_NAME = 'Boilerplate'
    REDIS_URL = 'redis://redis:6380/0'
    REDIS_HOST = 'redis'
    REDIS_PORT = 6380
    MQTT_BROKER_URL = 'mosquitto'
    MQTT_BROKER_PORT = 1883
    MQTT_KEEPALIVE = 0
    MQTT_TLS_ENABLED = False
    SECRET_KEY = "a secret"

class DevelopmentConfig(BaseConfig):
    """Development configuration."""
    WTF_CSRF_ENABLED = False
    CORS_HEADERS = 'Content-Type'

class TestingConfig(BaseConfig):
    """Testing configuration."""
    TESTING = True
    WTF_CSRF_ENABLED = False
    PRESERVE_CONTEXT_ON_EXCEPTION = False
