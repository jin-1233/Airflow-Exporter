#!/usr/bin/env python
# -*- coding:utf-8 -*-
import requests, os
import yaml
import logging
import time
# from random import randint
from lxml import etree
from flask import Flask, Response
from prometheus_client import Gauge, generate_latest, CollectorRegistry

logging.basicConfig(level=logging.INFO)
app = Flask(__name__)

config = yaml.safe_load(open(os.path.abspath(os.path.join(os.path.dirname(__file__), './config.yml')), 'r'))
node_list = config['nodes']


def parse_headers(s):
    s = s.strip()
    headers = [i.split(":", 1) for i in s.split("\n")]
    headers = {i[0].strip(): i[1].strip() for i in headers}
    return headers


headers = parse_headers("""Accept: text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9
Accept-Encoding: gzip, deflate
Accept-Language: zh-CN,zh;q=0.9,en;q=0.8
Cache-Control: max-age=0
Connection: keep-alive
Upgrade-Insecure-Requests: 1
User-Agent: Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/85.0.4183.102 Safari/537.36""")


# 获取失败任务列表
def get_faild_task_list(url):
    faild_task_page_html = requests.get(url=url, headers=headers, timeout=6).text
    p = etree.HTML(faild_task_page_html)
    failed_task_list = p.xpath('//td[@class="col-task_id"]/span/a[1]/text()')
    task_list = list(set(failed_task_list))

    return task_list


@app.route('/metrics')
def airflow_metrics():
    registry = CollectorRegistry()
    airflow_monitor = Gauge('airflow_monitor', 'a monitor of airflow', ['machine', 'remote_ip', 'dag', 'task'], registry=registry)
    start_time = time.time()
    for idx, node in enumerate(node_list):
        page_time = time.time()
        nginx_ip = node["nginx_ip"]
        node_name = node["name"]
        remote_ip = node["remote_ip"]
        dag_stats_url = f'http://{nginx_ip}/admin/airflow/dag_stats'
        node_url = f'http://{nginx_ip}/admin/'
        html = requests.get(url=node_url, headers=headers, timeout=6).text
        first_time = time.time()
        try:
            dag_response = requests.get(url=dag_stats_url, headers=headers, timeout=6)
            node_response = requests.get(url=node_url, headers=headers, timeout=6)
            dag_json = dag_response.json()
            html = node_response.text
        except Exception as e:
            logging.info("==========Requests Error==========")
            logging.info(f"node_index: {idx} \nnginx_ip: {nginx_ip}")
            logging.info(e)
            logging.info("================End===============")
        # logging.info(f"请求node：{time.time() - first_time}")
        p = etree.HTML(html)
        second_for_start = time.time()
        for dag_id in dag_json.keys():
            # for state_dict in dag_json[dag_id]:
            type_list = p.xpath(f'//input[@id="toggle-{dag_id}"]/@checked')
            if not type_list:
                continue
            if 'checked' in type_list:
                state_dict = dag_json[dag_id][-1]
                if state_dict['state'] == 'failed':
                    failed_count = state_dict['count']
                    dag_name = state_dict['dag_id']

                    if failed_count:
                        faild_task_url = f"http://{nginx_ip}/admin/taskinstance/?flt1_dag_id_equals={dag_name}&flt2_state_equals=failed"
                        start = time.time()
                        task_list = get_faild_task_list(faild_task_url)
                        logging.info(f"get_faild_task_list 运行时间: {time.time() - start}")
                        for task in task_list:
                            airflow_monitor.labels(machine=node_name, remote_ip=remote_ip, dag=dag_name, task=task).set(failed_count)
        # logging.info(f"second_for 运行时间：{time.time() - second_for_start}")
        # logging.info(f"采集完node：{time.time() - page_time}")
    logging.info(f"运行时间：{time.time() - start_time}")
    return Response(generate_latest(registry), mimetype='text/plain')


if __name__ == '__main__':
    app.run(host='0.0.0.0', port=9116)
