from airflow import DAG
from airflow.operators.python_operator import PythonOperator, BranchPythonOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.providers.telegram.operators.telegram import TelegramOperator
from datetime import datetime, timedelta
import json
import logging
import os
import sys

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../src')))

from airflow_comics.config import CONFIG
from airflow_comics.utils import save_message,save_comics,get_message,get_comics
from airflow_comics.scrape import scrape_comics_info


default_args = {
    'owner': 'kwchun0520',
    'start_date': datetime(2024, 7, 5, 0, 0),
    'schedule_interval': '@daily',
    'retries': 2,
    'retry_delay': timedelta(minutes=1)
}

def check_history(**context):
    return get_comics()


def get_comics_info(**context):
    comics_history = context['task_instance'].xcom_pull(task_ids='check_history')
    anything_new, all_comics_info = scrape_comics_info(comics_history)

    return anything_new, all_comics_info


def decide_what_to_do(**context):
    anything_new, _ = context['task_instance'].xcom_pull(task_ids='get_comics_info')
    if anything_new:
        return 'yes_generate_message'
    else:
        return 'no_do_nothing'
    

def generate_message(**context):
    _, all_comics_info = context['task_instance'].xcom_pull(task_ids='get_comics_info')

    message = 'New chapter available!\n\n'
    for comic_id, comic_info in all_comics_info.items():
        if comic_info['new_chapter']:
            message += '{}: (Previous:{} -> Latest:{})\n{}\n\n'.format(comic_info['name'], comic_info['previous_chapter'], comic_info['latest_chapter'], CONFIG['link'].replace("_id_",comic_id))
    save_message(message)
    

def save_comics_info(**context):
    _, all_comics_info = context['task_instance'].xcom_pull(task_ids='get_comics_info')
    comic_history = context['task_instance'].xcom_pull(task_ids='check_history')
    for id, info in all_comics_info.items():
        if info['new_chapter']:
            comic_history[id]['previous_chapter'] = info['latest_chapter']
    save_comics(comic_history)


def load_chat_id():
    return CONFIG['telegram']['chat_id']

def load_message():
    return get_message()


with DAG('comics', default_args=default_args, catchup=False) as dag:
    check_history = PythonOperator(
        task_id='check_history',
        python_callable=check_history,
        provide_context=True)

    get_comics_info = PythonOperator(
        task_id='get_comics_info',
        python_callable=get_comics_info,
        provide_context=True
        )
    
    decide_what_to_do = BranchPythonOperator(
        task_id='new_comics_available',
        python_callable=decide_what_to_do,
        provide_context=True
    )

    generate_notification = PythonOperator(
        task_id='yes_generate_message',
        python_callable=generate_message,
        provide_context=True
    )

    save_comics_info = PythonOperator(
        task_id='save_comics_info',
        python_callable=save_comics_info,
        provide_context=True
    )

    send_message = TelegramOperator(
        task_id='yes_send_message',
        telegram_conn_id='telegram_comics',
        text=load_message(),
        chat_id=load_chat_id()
    )

    do_nothing = DummyOperator(task_id='no_do_nothing')

    check_history >> get_comics_info >> decide_what_to_do
    decide_what_to_do >> generate_notification
    decide_what_to_do >> do_nothing
    generate_notification >> send_message >> save_comics_info

if __name__ == '__main__':
    dag.test()