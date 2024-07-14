from airflow_comics.constants import MESSAGE_PATH, COMICS_PATH
import json


def get_comics():
    with open(COMICS_PATH, 'r') as f:
        comics = json.load(f)
    return comics


def get_message():
    with open(MESSAGE_PATH, 'r') as fp:
        message = fp.read()
    return message


def save_comics(comics):
    with open(COMICS_PATH, 'w') as f:
        json.dump(comics, f)


def save_message(message):
    with open(MESSAGE_PATH, 'w') as f:
        f.write(message)