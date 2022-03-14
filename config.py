import os
from dotenv import load_dotenv

load_dotenv()


def env(param, default=''):
    return os.environ.get(param, default)


DOMAIN = env('DOMAIN')
APP_TOKEN = env('APP_TOKEN')
USERNAME = env('USERNAME')
PASSWORD = env('PASSWORD')
TIMEOUT = int(env('TIMEOUT'))
