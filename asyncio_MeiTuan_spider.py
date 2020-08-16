from urllib.parse import urlencode
from lxml import etree
from queue import Queue
import random
import requests
import base64
import aiohttp
import asyncio
import json
import time
import zlib
import re


class MeiTuanSpider:

    def __init__(self):
        self.IndexUrl = 'https://www.meituan.com/changecity/'
        self.PoiListUrl = "https://as.meituan.com/meishi/api/poi/getPoiList"
        self.origin_url = None
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) \
                           Chrome/83.0.4103.106 Safari/537.36'
        }
        self.acronym_queue = Queue()
        self.poi_queue = asyncio.Queue()
        self.ip_lst = []

    def get_acronym(self):
        res = requests.get(url=self.IndexUrl, headers=self.headers)
        cities_json = re.findall('"openCityList":(.*?),"recentCities"', res.text)[0]
        cities_dict = json.loads(cities_json)
        diclst = [cityDic for lettersCityLst in cities_dict for cityDicLst in lettersCityLst for cityDic in cityDicLst
                  if isinstance(cityDic, dict)]
        acronym_map = map(self.acronym_func, diclst)
        [self.acronym_queue.put(acronym_dict) for acronym_dict in acronym_map]

    def getpoi_urlslst(self):
        """:rtype: list"""
        while self.acronym_queue:
            acronym_dict = self.acronym_queue.get()
            self.origin_url = 'https://%s.meituan.com/meishi/' % acronym_dict['acronym']
            page_num = 1
            while True:
                origin_page = self.origin_url + 'pn%d/' % page_num
                query_params = {
                    'cityName': acronym_dict['name'],
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
                response = requests.get(url=self.PoiListUrl, headers=self.headers, params=query_params)
                bizpoilst = response.json()['data']['poiInfos']
                if bizpoilst:
                    poi_urlslist = [self.origin_url + str(biz_dic['poiId']) + '/' for biz_dic in bizpoilst]
                    page_num += 1
                    return poi_urlslist
                else:
                    break

    def get_ip(self):
        for num in range(1, 6):
            url = 'https://www.kuaidaili.com/free/inha/%d/' % num
            res = requests.get(url, headers=self.headers, timeout=5)
            html = etree.HTML(res.text)
            ips = html.xpath('//*[@id="list"]/table//tr/td[1]/text()')
            ports = html.xpath('//*[@id="list"]/table//tr/td[2]/text()')
            [self.ip_lst.append('%s:%s' % (ip, port)) for ip, port in zip(ips, ports)]
            time.sleep(1)

    async def get_info(self, poi_urlslist, proxy):
        async with aiohttp.ClientSession() as html:
            async with html.get(url=poi_urlslist, headers=self.headers, proxy=proxy, timeout=50) as response:
                page_text = await response.text(encoding='utf-8')
        biz_json = re.findall('"detailInfo":(.*?),"photos"', page_text)[0]
        info_dict = json.loads(biz_json)
        return info_dict

    @staticmethod
    def param_encode(param_dict):
        binary_encode = str(param_dict).encode()
        binary_zip = zlib.compress(binary_encode)
        encode_param = base64.b64encode(binary_zip).decode()
        return encode_param

    @staticmethod
    def acronym_func(dic):
        return {key: value for key, value in dic.items() if key == 'acronym' or key == 'name'}

    @staticmethod
    def callback(task):
        result = task.result()
        biz_info = '店铺名：%s  地址：%s  电话：%s  营业时间：%s  评分：%s  人均：%s' % (
            result['name'], result['address'], result['phone'],
            result['openTime'], result['avgScore'], result['avgPrice']
        )
        print(biz_info)

    def main(self):
        self.get_acronym()
        loop = asyncio.get_event_loop()
        urls = self.getpoi_urlslst()
        self.get_ip()
        proxy_ip = random.choice(self.ip_lst)['http']
        tasks = [asyncio.ensure_future(self.get_info(url, proxy_ip)) for url in urls]
        [task.add_done_callback(self.callback) for task in tasks]
        loop.run_until_complete(asyncio.wait(tasks))


if __name__ == '__main__':
    start_time = time.time()
    Spider = MeiTuanSpider()
    Spider.main()
    print('程序运行时间：%f秒' % (time.time() - start_time))
