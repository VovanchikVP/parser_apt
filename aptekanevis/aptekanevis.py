import asyncio
import random
import time

import pandas as pd

from bs4 import BeautifulSoup
import aiohttp
from urllib.parse import urljoin
from typing import List
from util import print_progress_bar


class ParserAptekanevis:
    URL = 'https://aptekanevis.ru/'
    PAGE = '?PAGEN_1={num}'
    PHARMACY = 'Невис'

    def __init__(self, max_request: int = 1):
        self.data = {'name': [], 'cost': [], 'pharmacy': [], 'coordinates': [],
                     'cost_in_pharmacy': [], 'count_in_pharmacy': [], 'old_cost': []}
        self.max_request = max_request

    async def run_parser(self):
        """Запуск парсера"""
        async with aiohttp.ClientSession() as session:
            catalogs_urls = await self._get_category_urls(session)
            for name, url in catalogs_urls.items():
                await self._pars_product(url, session)
        df = pd.DataFrame(self.data)
        df.to_excel(f'{self.PHARMACY}.xlsx')

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
        pending = [asyncio.create_task(self._fetch(session, f"{url_cat}{i}")) for i in pages]

        progress = 0
        max_progress = len(pending)
        print(f'Загрузка: {url_cat}')
        print_progress_bar(progress, max_progress, prefix='Progress:', suffix='Complete', length=50)
        while pending:
            ind = random.randrange(len(pending))
            sub_pending = pending[ind]
            del pending[ind]
            try:
                await self._get_product_data(await sub_pending)
            except AttributeError:
                pending.append(sub_pending)
                time.sleep(30)
                print(f'error_{ind}')
                print_progress_bar(progress, max_progress, prefix='Progress:', suffix='Complete', length=50)
                continue
            progress += 1
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
                name_product = content.find('a', class_='name_link').text.strip()
                manufacturer = content.find('a', class_='manufacturer')
                manufacturer = manufacturer.text.strip() if manufacturer else manufacturer
                name = f"{name_product}_{manufacturer}" if manufacturer else name_product
                super_cost = product.find('span', class_='super_price')
                super_cost = super_cost.text.strip() if super_cost else super_cost
                simple_price = product.find('span', class_='simple_price')
                simple_price = simple_price.text.strip() if simple_price else simple_price
                cost = super_cost or simple_price
                old_cost = product.find('span', class_='old_price')
                old_cost = old_cost.text.strip() if old_cost else old_cost
                self.data['name'].append(name)
                self.data['cost'].append(cost)
                self.data['pharmacy'].append(self.PHARMACY)
                self.data['coordinates'].append(None)
                self.data['cost_in_pharmacy'].append(cost)
                self.data['count_in_pharmacy'].append(None)
                self.data['old_cost'].append(old_cost)
            except AttributeError:
                print(product)
