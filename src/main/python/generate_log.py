# -*- coding: utf-8 -*-
import random
import sys
import time


url_path = [
    "class/259.html",
    "class/3324.html",
    "class/4234.html",
    "class/43.html",
    "class/12.html",
    "cource/2313.html",
    "learn/1011.html",
    "class/789.html",
    "class/803.html",
    "class/890.html",
    "cource/890.html",
    "learn/45.html",
]

ip = [12,32,12.65,122,124,168,166,145,43,87,98,108,178,23,67,87]
status_codes = [404,500,200]
http_refers = [
    "https://www.baidu.com/s?wd={query}",
    "https://www.google.com.hk/search?q={query}",
    "https://search.yahoo.com/search?p={query}",
    "https://www.so.com/s?q=a{query}",
    "https://cn.bing.com/search?q={query}",
]

search_keyword = [
    "Spark Streaming",
    "Hadoop",
    "大数据",
    "Spark SQL实战",
    "Storm 实战",
    "Saprk实战"
]

def search_key():
    if random.uniform(0,1) > 0.4:
        return "-"
    refer = random.sample(http_refers,1)[0]
    key = random.sample(search_keyword,1)[0]
    return refer.format(query=key)

def random_status():
    return random.sample(status_codes,1)[0]

def random_url():
    return random.sample(url_path,1)[0]

def random_ip():
    ip_sample = random.sample(ip,4)
    return ".".join([str(item) for item in ip_sample])

def get_log(times = 10):
    with open("/Users/harold/Documents/Code/sparkdemo/out/server_log", 'a+') as f:
        while (times>=1) :
            time_str = time.strftime("%Y-%m-%d %H:%M:%S",time.localtime())
            query_log = "{times}\t{url}\t{ip}\t{search_key}\t{status_code}".format(url = random_url(), ip = random_ip(), status_code = random_status(), search_key=search_key(), times=time_str)
            f.write(query_log + "\n")
            times -= 1

if __name__ == '__main__':
    get_log()
