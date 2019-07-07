import argparse

import yaml


def _get_config_file():
    parser = argparse.ArgumentParser(description='Archive media files to S3')
    parser.add_argument('-c', dest='config', type=str, help='Path to config file', required=True)
    args = parser.parse_args()
    return args.config


with open(_get_config_file(), 'rt') as f:
    conf = yaml.safe_load(f)

SALT = conf['salt']

PASSWORD = conf['encryption_password']

BUCKET = conf['bucket']

STORAGE_CLASS = conf['storage_class']

METADATA_DIR = conf['metadata_dir']

PATHS = conf['paths']

UPLOAD_MBIT_PER_SECOND = conf['upload_speed']

STOP_TIME_STR = conf['stop_time']