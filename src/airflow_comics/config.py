import os
import yaml
from airflow_comics.constants import CONFIG_PATH

def load_config():
    with open(CONFIG_PATH, 'r') as fp:
        config = yaml.load(fp, Loader=yaml.FullLoader)
    return config

CONFIG = load_config()

