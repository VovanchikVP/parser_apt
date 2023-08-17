import asyncio
import random
import time

import pandas as pd

from bs4 import BeautifulSoup
import aiohttp
from urllib.parse import urljoin
from typing import List
from util import print_progress_bar
from aptekanevis.config import ADDRESS


class ParserAptekanevis:
    URL = 'https://aptekanevis.ru/'
    PAGE = '?PAGEN_1={num}'
    PHARMACY = 'Невис'

    def __init__(self, max_request: int = 10):
        self.data = {'name': [], 'producer': [], 'cost': [], 'url': [], 'address': []}
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
        """Формирование таблицы"""
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

    async def _get_address_data(self, session: aiohttp.ClientSession, url: str, ind: int):
        async with session.get(url) as result:
            result = await result.text()
        soup = BeautifulSoup(result, 'lxml')
        data = soup.find_all('div', class_='apteka__list__item apteka__list__item-visible')
        all_address = []
        for i in data:
            address = i.find('div', class_='apteka__item__address').text.strip()
            all_address.append(address)
        self.data['address'][ind] = all_address
        self.all_address.update(all_address)

    async def _get_address(self, session: aiohttp.ClientSession) -> None:
        """Получение адресов аптек с наличием препаратов"""
        print('Получение данных о наличии в аптеках')
        progress = 0
        max_progress = len(self.data['url'])
        print_progress_bar(progress, max_progress, prefix='Progress:', suffix='Complete', length=50)
        self.data['address'] = [None for _ in range(len(self.data['url']))]

        pending = [self._get_address_data(session, i, num) for num, i in enumerate(self.data['url'])]

        while pending:
            sub_pending = [asyncio.create_task(i) for i in pending[:self.max_request]]
            del pending[:self.max_request]
            await asyncio.gather(*sub_pending)

            progress += len(sub_pending)
            print_progress_bar(progress, max_progress, prefix='Progress:', suffix='Complete', length=50)

    async def _get_category_urls(self, session: aiohttp.ClientSession) -> dict:
        """Получение данных категорий"""
        async with session.get(self.URL) as result:
            result = await result.text()
        soup = BeautifulSoup(result, 'lxml')
        data = soup.find('ul', class_='header__catnav__list')
        links = data.find_all('a', class_='header__catnav__link')
        menu = {}
        for i in links:
            sub_menu = await self._get_sub_category_urls(session, i.attrs['href'])
            if not sub_menu:
                text = i.find('span').text.strip()
                menu[text] = i.attrs['href']
            else:
                menu.update(sub_menu)
        return menu

    async def _get_sub_category_urls(self, session: aiohttp.ClientSession, url: str) -> dict:
        """Получение подкатегорий"""
        url_cat = urljoin(self.URL, url)
        result = await self._fetch(session, url_cat)
        soup = BeautifulSoup(result, 'lxml')
        data = soup.find('ul', class_='subcats__list')
        menu = {}
        for i in data.find_all('a', class_='text-transform-first'):
            text = i.text.strip()
            menu[text] = i.attrs['href']
        return menu

    async def _pars_product(self, url: str, session: aiohttp.ClientSession) -> None:
        """Получает данные о всех товарах по ссылке категории"""
        url_cat = urljoin(self.URL, url)
        result = await self._fetch(session, url_cat)
        pages = await self._get_all_pages(result)
        await self._get_product_data(data=result)
        pending = [self._fetch(session, f"{url_cat}{i}") for i in pages]

        print(f'Загрузка: {url_cat}')
        progress = 0
        max_progress = len(pending)
        print_progress_bar(progress, max_progress, prefix='Progress:', suffix='Complete', length=50)

        while pending:
            sub_pending = [asyncio.create_task(i) for i in pending[:self.max_request]]
            del pending[:self.max_request]
            result = await asyncio.gather(*sub_pending)
            for i in result:
                await self._get_product_data(i)
            progress += len(sub_pending)
            print_progress_bar(progress, max_progress, prefix='Progress:', suffix='Complete', length=50)

    @staticmethod
    async def _fetch(session: aiohttp.ClientSession, url: str) -> str:
        """Запрос данных"""
        async with session.get(url) as result:
            return await result.text()

    async def _get_all_pages(self, data: str) -> List[str]:
        """Получение всех страниц в категории"""
        soup = BeautifulSoup(data, 'lxml')
        pages = soup.find('div', class_='catalog__pagination')
        if not pages:
            return []
        last_page = int(pages.find_all('a')[-2].text.strip())
        return [self.PAGE.format(num=i+1) for i in range(last_page) if i]

    async def _get_product_data(self, data: str) -> None:
        """Получение данных продуктов со страницы"""
        soup = BeautifulSoup(data, 'lxml')
        all_products = soup.find('div', class_='product__list')
        if not all_products:
            return None
        for product in all_products.find_all('div', class_='product__box__wrapper'):
            try:
                content = product.find('div', class_='product__descr')
                product_obj = content.find('a', class_='name_link')
                name_product = product_obj.text.strip()
                product_url = urljoin(self.URL, product_obj.attrs['href'])
                manufacturer = content.find('a', class_='manufacturer')
                manufacturer = manufacturer.text.strip() if manufacturer else manufacturer
                super_cost = product.find('span', class_='super_price')
                super_cost = super_cost.text.strip() if super_cost else super_cost
                simple_price = product.find('span', class_='simple_price')
                simple_price = simple_price.text.strip() if simple_price else simple_price
                cost = super_cost or simple_price
                self.data['name'].append(name_product)
                self.data['producer'].append(manufacturer)
                self.data['cost'].append(cost)
                self.data['url'].append(product_url)

            except AttributeError:
                print(product)
