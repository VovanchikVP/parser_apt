import copy
import json
import random
import time

import pandas as pd

from bs4 import BeautifulSoup
import aiohttp
from urllib.parse import urljoin
from util import print_progress_bar
from acmespb.config import REGION, NAME_OF_DRUGS
from typing import List, Dict


class ParserAcmespb:
    URL = 'https://www.acmespb.ru/'
    SEARCH_URL = 'https://www.acmespb.ru/search.php'
    MAP_DATA = 'https://www.acmespb.ru/lib/ajax.php'
    PHARMACY = 'Экми'
    DATA_POST = {
        'free_str': '',
        'name': '',
        'page': '',
        'whatever': 1,
        'order': 'price',
        'sreg': REGION,
        'apt_net': ''
    }

    def __init__(self, sleep: int = 1):
        self.data = {'name': [], 'cost': [], 'pharmacy': [], 'coordinates': [],
                     'cost_in_pharmacy': [], 'count_in_pharmacy': []}
        self.result = {}
        self.sleep = sleep
        self.map_data = {}

    async def run_parser(self):
        """Запуск парсера"""
        async with aiohttp.ClientSession() as session:
            await self._get_data_prepar(session)

    async def _get_data(self, data: str, page_name: str):
        """Получение данных"""
        soup = BeautifulSoup(data, 'lxml')
        result = soup.find('div', class_="row result result__table")
        count_rows = result.find('div', {'id': 'container'})
        count_rows = count_rows.find_all('div', class_='trow')
        if page_name in self.result:
            values = self.result[page_name]
        else:
            values = copy.deepcopy(self.data)
            self.result[page_name] = values
        for i in count_rows[1:]:
            name = i.find('div', class_='cell name').find('p').text.strip()
            pharm = i.find('div', class_='cell pharm').find('a').text.strip()
            address = i.find('div', class_='cell address').find('a').text.strip()
            coordinates = self.map_data.get(address)
            cost = i.find('div', class_='cell pricefull').text.strip()
            values['name'].append(name)
            values['cost'].append(cost)
            values['pharmacy'].append(pharm)
            values['coordinates'].append(coordinates)
            values['cost_in_pharmacy'].append(cost)
            values['count_in_pharmacy'].append(None)

    @staticmethod
    async def _fetch_requests(session: aiohttp.ClientSession, url: str, data: dict) -> str:
        async with session.post(url, data=data) as result:
            data = await result.content.read()
            return data.decode("utf-8")

    @staticmethod
    async def _get_apt_net(data: str) -> List[Dict[str, str]]:
        """Получение кода аптек"""
        soup = BeautifulSoup(data, 'lxml')
        select = soup.find('select', {'id': 'apt_net'})
        result = []
        for num, i in enumerate(select.find_all('option')):
            if num:
                result.append({'code': i.attrs['value'], 'name': i.text.strip()})
        return result

    async def _fetch_search(self, session: aiohttp.ClientSession, name: str) -> str:
        async with session.post(self.SEARCH_URL, data={'free_str': name, 'sreg': REGION}) as result:
            data = await result.content.read()
            return data.decode("utf-8")

    async def _get_url_data(self, data: str) -> list:
        """Добавление ссылок для поиска препаратов"""
        soup = BeautifulSoup(data, 'lxml')
        urls = soup.find('ul', class_='similars')
        result = []
        for i in urls.find_all('a'):
            result.append({'url': urljoin(self.URL, i.attrs['href']), 'name': i.text.strip()})
        return result

    async def _get_data_prepar(self, session: aiohttp.ClientSession):
        """Получение данных по препаратам"""
        async for url_data in self._search(session):
            print(f"Загрузка данных: {url_data['name']}")
            async with session.get(url_data['url']) as result:
                await result.content.read()
            self.DATA_POST['free_str'] = url_data['name']
            self.DATA_POST['name'] = url_data['url'].split('/')[-1]
            apt = await self._get_apt_net(await self._fetch_requests(session, url_data['url'], self.DATA_POST))

            progress = 0
            max_progress = len(apt)
            print_progress_bar(progress, max_progress, prefix='Progress:', suffix='Complete', length=50)

            while apt:
                ind = random.randrange(len(apt))
                ap = apt[ind]
                del apt[ind]
                self.DATA_POST['apt_net'] = ap['code']
                data = await self._fetch_requests(session, url_data['url'], self.DATA_POST)
                try:
                    await self._get_data(data, ap['name'])
                except AttributeError:
                    print(f"Нет данных по препарату {url_data['name']}", url_data['url'], ap['name'])

                progress += 1
                print_progress_bar(progress, max_progress, prefix='Progress:', suffix='Complete', length=50)

                time.sleep(self.sleep)
            print(f"Загрузка данных: {url_data['name']} - Завершина")
        await self._create_df()

    async def _search(self, session: aiohttp.ClientSession):
        """Поиск ссылок для препаратов"""
        async with session.get(self.URL) as result:
            await result.content.read()
        async with session.post(self.MAP_DATA, data={'func': 'mapPharms'}) as result:
            map_data = await result.content.read()
            map_data = json.loads(map_data.decode("utf-8"))
            for i in map_data:
                self.map_data[i['apt_adr']] = f"{i['apt_lat']}, {i['apt_lng']}"
        for i in NAME_OF_DRUGS:
            data = await self._fetch_search(session, i)
            result = []
            for j in await self._get_url_data(data):
                result.append(j)

            while result:
                ind = random.randrange(len(result))
                j = result[ind]
                del result[ind]
                yield j

    async def _create_df(self):
        """Формирование DF и сохранение в excel"""
        with pd.ExcelWriter(f'{self.PHARMACY}.xlsx') as writer:
            for page_name, value in self.result.items():
                sh_name = page_name if len(page_name) < 30 else page_name[:30]
                df = pd.DataFrame(value)
                df.to_excel(writer, sheet_name=sh_name)


