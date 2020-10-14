import os

base_dir = os.path.abspath(os.path.dirname(__file__))


class Config(object):
    DEBUG = True
    TESTING = False
    CSRF_ENABLED = True
    SECRET_KEY = 'my secret key'
    SQLALCHEMY_DATABASE_URI = os.environ['DATABASE_URL']
    SQLALCHEMY_TRACK_MODIFICATIONS = False


class ProductionConfig(Config):
    DEBUG = False


class TestConfig(Config):
    TESTING = True


class DevConfig(Config):
    DEBUG = True
    DEVELOPMENT = True


config_by_name = dict(dev=DevConfig,
                      test=TestConfig,
                      prod=ProductionConfig)
