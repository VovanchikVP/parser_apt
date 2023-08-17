import asyncio
import pandas as pd
import re

from bs4 import BeautifulSoup
import aiohttp
from urllib.parse import urljoin
from typing import List, Tuple
from util import print_progress_bar
from lekopttorg.config import ADDRESS


class ParserLekopttorg:
    URL = 'https://lekopttorg.ru/'
    PAGE = '?PAGEN_3={num}'
    PHARMACY = 'ЛекОптТорг'
    URL_ADDRESS = 'https://lekopttorg.ru/bitrix/services/main/ajax.php?mode=class&c=webit.custom%3Astores.list&action=getStores'

    def __init__(self, max_request: int = 10):
        self.data = {'name': [], 'producer': [], 'cost': [], 'url': [], 'address': [], 'id': []}
        self.max_request = max_request
        self.all_address = set()

    async def run_parser(self):
        """Запуск парсера"""
        async with aiohttp.ClientSession() as session:
            catalogs_urls = await self._get_category_urls(session)
            for name, url in catalogs_urls.items():
                await self._pars_product(url, session)
            await self._get_address(session)
        await self._create_df()

    async def _create_df(self) -> None:
        """Формирование структыры таблицы"""
        self.data.update({i: 0 for i in self.all_address})
        df = pd.DataFrame(self.data)
        if not ADDRESS:
            all_address = list(self.all_address)
        else:
            all_address = list(ADDRESS)
        all_address.sort()
        for i in all_address:
            df[i] = df.apply(lambda row: row['cost'] if i in row['address'] else 0, axis=1)
        df['min'] = df['cost'].copy()
        df['max'] = df['cost'].copy()
        result = df[['name', 'producer', 'min', 'max'] + all_address]
        result.to_excel(f'result/{self.PHARMACY}.xlsx')

    async def _get_address_data(self, data: Tuple[str, int], session: aiohttp.ClientSession) -> None:
        """Получение данных адресов"""
        csrf = re.search(r"'bitrix_sessid':'(.+)'", data[0])[1]
        body = {
            'id': self.data['id'][data[1]],
            'filter': {'hours24': False, 'optika': False, 'search': '', 'view': 'map'}
        }
        headers = {
            "Referer": self.data['url'][data[1]],
            "X-Bitrix-Csrf-Token": csrf
        }
        all_address = []
        async with session.post(self.URL_ADDRESS, headers=headers, json=body) as result:
            address = await result.json()
            for i in address['data']['STORES']:
                all_address.append(i['NAME'])
        self.all_address.update(all_address)
        self.data['address'][data[1]] = all_address

    async def _get_address(self, session: aiohttp.ClientSession):
        """Получение адресов"""
        pending = [self._fetch_address(session, i, ind) for ind, i in enumerate(self.data['url'])]
        self.data['address'] = [[] for _ in range(len(self.data['url']))]

        print('Получение данных о наличии в аптеках')
        progress = 0
        max_progress = len(pending)
        print_progress_bar(progress, max_progress, prefix='Progress:', suffix='Complete', length=50)

        while pending:
            sub_pending = [asyncio.create_task(i) for i in pending[:self.max_request]]
            del pending[:self.max_request]
            while sub_pending:
                done, sub_pending = await asyncio.wait(
                    sub_pending, return_when=asyncio.FIRST_COMPLETED
                )

                for done_task in done:
                    get_data = await done_task
                    await self._get_address_data(get_data, session)
                progress += len(done)
                print_progress_bar(progress, max_progress, prefix='Progress:', suffix='Complete', length=50)

    async def _get_category_urls(self, session: aiohttp.ClientSession) -> dict:
        """Получение данных категорий"""
        async with session.get(self.URL) as result:
            result = await result.text()
        soup = BeautifulSoup(result, 'lxml')
        data = soup.find('nav', class_='header__nav invis-scroll')
        links = data.find_all('a', class_='header__link')
        menu = {}
        for i in links:
            menu[i.text.strip()] = i.attrs['href']
        return menu

    async def _pars_product(self, url: str, session: aiohttp.ClientSession) -> None:
        """Получает данные о всех товарах по ссылке категории"""
        url_cat = urljoin(self.URL, url)
        result = await self._fetch(session, url_cat)
        pages = await self._get_all_pages(result)
        await self._get_product_data(data=result)
        pending = [self._fetch(session, f"{url_cat}{i}") for i in pages]

        progress = 0
        max_progress = len(pending)
        print_progress_bar(progress, max_progress, prefix='Progress:', suffix='Complete', length=50)
        while pending:
            sub_pending = [asyncio.create_task(i) for i in pending[:self.max_request]]
            del pending[:self.max_request]
            while sub_pending:
                done, sub_pending = await asyncio.wait(
                    sub_pending, return_when=asyncio.FIRST_COMPLETED
                )

                for done_task in done:
                    await self._get_product_data(await done_task)
                progress += len(done)
                print_progress_bar(progress, max_progress, prefix='Progress:', suffix='Complete', length=50)

    @staticmethod
    async def _fetch(session: aiohttp.ClientSession, url: str) -> str:
        """Запрос данных"""
        async with session.get(url) as result:
            return await result.text()

    @staticmethod
    async def _fetch_address(session: aiohttp.ClientSession, url: str, ind: int) -> Tuple[str, int]:
        """Запрос данных"""
        async with session.get(url) as result:
            data = await result.text()
            return data, ind

    async def _get_all_pages(self, data: str) -> List[str]:
        """Получение всех страниц в категории"""
        soup = BeautifulSoup(data, 'lxml')
        pages = soup.find('div', class_='pagintaion__number')
        last_page = int(pages.find_all('a')[-1].text.strip())
        return [self.PAGE.format(num=i+1) for i in range(last_page) if i]

    async def _get_product_data(self, data: str) -> None:
        """Получение данных продуктов со страницы"""
        soup = BeautifulSoup(data, 'lxml')
        all_products = soup.find('div', class_='catalog-line')
        for product in all_products.find_all('div', class_='product'):
            try:
                product_id = int(product.attrs['id'].split('-')[-1])
                content = product.find('div', class_='product__content')
                product_obj = content.find('a')
                product_name = product_obj.text.strip()
                product_url = urljoin(self.URL, product_obj.attrs['href'])
                producer_name = content.find('span', class_='val').text.strip()
                cost = product.find('span', class_='price__regular').text.strip()
                self.data['name'].append(product_name)
                self.data['producer'].append(producer_name)
                self.data['cost'].append(cost)
                self.data['url'].append(product_url)
                self.data['id'].append(product_id)
            except AttributeError:
                print(product)
