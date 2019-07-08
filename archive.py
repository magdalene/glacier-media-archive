from datetime import datetime, timedelta
import hashlib
import json
import logging
import os
import subprocess
import sys

import boto3

from config import *


FORMAT = '%(asctime)-15s %(message)s'
logging.basicConfig(format=FORMAT)
logger = logging.getLogger('glacier-media-archive')
logger.setLevel(logging.INFO)


s3 = boto3.client('s3')

stop_time = None


def _set_stop_time():
    global stop_time
    now = datetime.now()
    stop_time = datetime.strptime('{year}-{month}-{day}T{STOP_TIME_STR}'.format(
        year=now.year,
        month=now.month,
        day=now.day,
        STOP_TIME_STR=STOP_TIME_STR
    ), '%Y-%m-%dT%H:%M')
    if stop_time < now:
        stop_time = stop_time + timedelta(days=1)


def get_hashed_filename(filename):
    return hashlib.sha256((SALT + filename).encode('utf-8')).hexdigest()


def encrypt_file(input_filename, output_filename, password=PASSWORD):
    subprocess.call(['openssl', 'enc', '-aes-128-cbc', '-salt', '-in',
                     input_filename, '-out', output_filename, '-k', PASSWORD])


def upload_file(local_filepath, key_prefix):
    filename = os.path.split(local_filepath)[1]
    s3.upload_file(local_filepath, BUCKET, '{key_prefix}/{filename}'.format(key_prefix=key_prefix, filename=filename), ExtraArgs={'StorageClass': STORAGE_CLASS})


def write_metadata(hashed_filename, filename, prefix, original_path, _type):
    if not os.path.exists(os.path.join(METADATA_DIR, prefix)):
        os.makedirs(os.path.join(METADATA_DIR, prefix))
    with open(os.path.join(METADATA_DIR, prefix, hashed_filename), 'wt') as f:
        json.dump({
            "original_path": original_path,
            "filename": filename,
            "type": _type
        }, f)


def check_time(directory, path_in_dir):
    file_bytes = os.stat(os.path.join(directory, path_in_dir)).st_size
    time_to_upload_seconds = file_bytes * 8 / (1000000 * int(UPLOAD_MBIT_PER_SECOND))
    now = datetime.now()
    time_left = (stop_time - now).seconds
    return stop_time > now and time_left > time_to_upload_seconds


def is_already_uploaded(hashed_filename, prefix):
    return os.path.exists(os.path.join(METADATA_DIR, prefix, hashed_filename))


def archive_file(directory, path_in_dir, prefix, _type):
    hashed_filename = get_hashed_filename(path_in_dir)
    encrypted_filepath = os.path.join('/tmp', hashed_filename)
    if is_already_uploaded(hashed_filename, prefix):
        return
    logger.info("Working on archiving {_type} {directory}/{path_in_dir}".format(
        _type=_type, directory=directory, path_in_dir=path_in_dir))
    original_path = os.path.join(directory, path_in_dir)
    encrypt_file(original_path, encrypted_filepath)
    upload_file(encrypted_filepath, prefix)
    write_metadata(hashed_filename, path_in_dir, prefix, original_path, _type)
    os.remove(encrypted_filepath)
    logger.info("Archived {_type} {directory}/{path_in_dir} ({prefix}/{hashed_filename}".format(
        _type=_type,
        directory=directory,
        prefix=prefix,
        hashed_filename=hashed_filename,
        path_in_dir=path_in_dir
    ))


def get_files(directory):
    if not directory.endswith('/'):
        directory = '{directory}/'.format(directory=directory)
    relative_filepaths = []
    for root, dirs, files in os.walk(directory):
        for f in files:
            relative_dir = root.replace(directory, '', 1)
            relative_filepaths.append(os.path.join(relative_dir, f))
    return sorted(relative_filepaths)


def main():
    _set_stop_time()
    logger.info("Stopping by {}".format(stop_time))
    for path_info in PATHS:
        filenames = get_files(path_info['path'])
        for filename in filenames:
            if not check_time(path_info['path'], filename):
                logger.info("Not enough time to finish {path}/{filename}, moving on to the next one!".format(
                    path=path_info['path'],
                    filename=filename
                ))
                continue
            archive_file(path_info['path'], filename, path_info['prefix'], path_info['type'])

if __name__ == '__main__':
    main()
