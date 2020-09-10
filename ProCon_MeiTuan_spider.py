from threading import Thread, Condition, currentThread
from urllib.parse import urlencode
from lxml import etree
import random
import requests
import time
import re
import json
import zlib
import base64
from queue import Queue

condition = Condition()


class Producer(Thread):
    def __init__(self, city_que, biz_que, ip_lst):
        super(Producer, self).__init__()
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) \
                           Chrome/83.0.4103.116 Safari/537.36'
        }
        self.PoiListUrl = "https://as.meituan.com/meishi/api/poi/getPoiList"
        self.city_que = city_que
        self.biz_que = biz_que
        self.ip_lst = ip_lst

    def run(self):
        while True:
            if condition.acquire():
                if self.city_que.empty():
                    print('爬虫结束，程序停止')
                    condition.release()
                    break
                acronym_dict = self.city_que.get()
                if self.biz_que.qsize() >= 105:
                    print('商户队列满了，停止放入')
                    condition.wait()
                else:
                    self.get_token(acronym_dict)
                    print(currentThread().ident, '生产了,目前共', bizQue.qsize(), '个')
                condition.notify()
                condition.release()
                time.sleep(random.randrange(3))

    def get_token(self, acronym_dic):
        origin_url = 'https://%s.meituan.com/meishi/' % acronym_dic['acronym']
        page_num = 1
        while True:
            origin_page = origin_url + 'pn%d/' % page_num
            query_params = {
                'cityName': acronym_dic['name'],
                'cateId': '0',
                'areaId': '0',
                'sort': '',
                'dinnerCountAttrId': '',
                'page': str(page_num),
                'userId': '',
                'uuid': '06773764-157e-45cb-b303-dd4d7778f020',
                'platform': '1',
                'partner': '126',
                'originUrl': origin_page,
                'riskLevel': '1',
                'optimusCode': '10'
            }

            format_query = urlencode(query_params)
            sign = self.param_encode(format_query)
            meituan_ipjs = {
                'rId': 100900,
                'ver': '1.0.6',
                "ts": int(time.time() * 1000),
                "cts": int(time.time() * 1000 + 100),
                "brVD": [1536, 770],
                "brR": [[1536, 864], [1536, 842], 24, 24],
                "bI": [origin_page, ""],
                'mT': [],
                'kT': [],
                'aT': [],
                'tT': [],
                'aM': '',
                'sign': sign
            }

            token = self.param_encode(meituan_ipjs)
            query_params["_token"] = token
            self.headers['Referer'] = origin_page
            bizurlslst = self.getpoi_lst(query_params)
            if not bizurlslst:
                break
            [self.biz_que.put(origin_url + str(biz_dic['poiId']) + '/') for biz_dic in bizurlslst]
            page_num += 1
            break

    def getpoi_lst(self, query_params):
        try:
            proxy = {'http': random.choice(self.ip_lst)}
            response = requests.get(url=self.PoiListUrl, headers=self.headers, proxies=proxy, params=query_params)
            bizurlslst = response.json()['data']['poiInfos']
            return bizurlslst
        except Exception as e:
            print(e)
            self.getpoi_lst(query_params)

    @staticmethod
    def param_encode(param_dict):
        binary_encode = str(param_dict).encode()
        binary_zip = zlib.compress(binary_encode)
        encode_param = base64.b64encode(binary_zip).decode()
        return encode_param


class Consumer(Thread):
    def __init__(self, biz_que, ip_lst):
        super(Consumer, self).__init__()
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) \
                                   Chrome/83.0.4103.106 Safari/537.36'
        }
        self.Client = pymongo.MongoClient('mongodb://%s:%s@%s:%s/' % ('root', None, 'localhost', 27017))
        self.Collection = self.Client['MeiTuan']['Data']
        self.biz_que = biz_que
        self.ip_lst = ip_lst

    def run(self):
        while True:
            if condition.acquire():
                if self.biz_que.empty():
                    condition.release()
                    break
                if self.biz_que.qsize() < 1:
                    print('biz_que 为空，停止获取')
                    condition.wait()
                else:
                    biz_url = self.biz_que.get()
                    self.get_info(biz_url)
                condition.notify()
                condition.release()
                time.sleep(random.randrange(2))

    def get_info(self, url):
        try:
            print(currentThread().ident, '消费了', url, '还剩', bizQue.qsize(), '个')
            proxy = {'http': random.choice(self.ip_lst)}
            res = requests.get(url, headers=self.headers, proxies=proxy)
            biz_json = re.findall('"detailInfo":(.*?),"photos"', res.text)[0]
            info_dict = json.loads(biz_json)
            biz_info = '店铺名：%s  地址：%s  电话：%s  营业时间：%s  评分：%s  人均：%s' % (
                info_dict['name'], info_dict['address'], info_dict['phone'],
                info_dict['openTime'], info_dict['avgScore'], info_dict['avgPrice']
            )
            print(biz_info)
            
            biz_dict = {
                '店铺名': info_dict['name'],
                '地址': info_dict['address'],
                '电话': info_dict['phone'],
                '营业时间': info_dict['openTime'],
                '评分': info_dict['avgScore'],
                '人均': info_dict['avgPrice']
            }
        self.Collection.insert_one(biz_dict)
        except Exception as e:
            self.get_info(url)
            print(e)


class MakeCityUrl:
    def __init__(self):
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) \
                                   Chrome/83.0.4103.106 Safari/537.36'
        }
        self.url = 'https://www.meituan.com/changecity/'
        self.ip_lst = []

    def get_city(self, queue):
        res = requests.get(self.url, headers=self.headers)
        cities_json = re.findall('"openCityList":(.*?),"recentCities"', res.text)[0]
        cities_dict = json.loads(cities_json)
        diclst = [cityDic for lettersCityLst in cities_dict for cityDicLst in lettersCityLst for cityDic in cityDicLst
                  if isinstance(cityDic, dict)]
        acronym_map = map(self.acronym_func, diclst)
        [queue.put(acronym_dict) for acronym_dict in acronym_map]

    @staticmethod
    def acronym_func(dic):
        return {key: value for key, value in dic.items() if key == 'acronym' or key == 'name'}

    def get_ip(self):
        for num in range(1, 5):
            url = 'https://www.kuaidaili.com/free/inha/%d/' % num
            res = requests.get(url, headers=self.headers, timeout=5)
            html = etree.HTML(res.text)
            ips = html.xpath('//*[@id="list"]/table//tr/td[1]/text()')
            ports = html.xpath('//*[@id="list"]/table//tr/td[2]/text()')
            [self.ip_lst.append('%s:%s' % (ip, port)) for ip, port in zip(ips, ports)]
            time.sleep(2)
            print('IP获取成功，数量%d' % len(self.ip_lst))


def main():
    city = MakeCityUrl()
    city.get_ip()
    city.get_city(cityQue)
    for i in range(2):
        product = Producer(cityQue, bizQue, city.ip_lst)
        product.start()

    for i in range(5):
        consume = Consumer(bizQue, city.ip_lst)
        consume.start()


if __name__ == '__main__':
    cityQue = Queue()
    bizQue = Queue()
    main()
