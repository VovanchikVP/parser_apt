import asyncio
import pandas as pd

from bs4 import BeautifulSoup
import aiohttp
from urllib.parse import urljoin
from typing import List
from util import print_progress_bar


class ParserLekopttorg:
    URL = 'https://lekopttorg.ru/'
    PAGE = '?PAGEN_3={num}'
    PHARMACY = 'ЛекОптТорг'

    def __init__(self, max_request: int = 1):
        self.data = {'name': [], 'cost': [], 'pharmacy': [], 'coordinates': [],
                     'cost_in_pharmacy': [], 'count_in_pharmacy': []}
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
        pending = [asyncio.create_task(self._fetch(session, f"{url_cat}{i}")) for i in pages]

        progress = 0
        max_progress = len(pending)
        print_progress_bar(progress, max_progress, prefix='Progress:', suffix='Complete', length=50)
        while pending:
            done, pending = await asyncio.wait(
                pending, return_when=asyncio.FIRST_COMPLETED
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
                content = product.find('div', class_='product__content')
                name = f"{content.find('a').text.strip()}_{content.find('span', class_='val').text.strip()}"
                cost = product.find('span', class_='price__regular').text.strip()
                self.data['name'].append(name)
                self.data['cost'].append(cost)
                self.data['pharmacy'].append(self.PHARMACY)
                self.data['coordinates'].append(None)
                self.data['cost_in_pharmacy'].append(cost)
                self.data['count_in_pharmacy'].append(None)
            except AttributeError:
                print(product)
