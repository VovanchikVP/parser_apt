import requests
import copy
from bs4 import BeautifulSoup
import pandas as pd
from papteki.config import ADDRESS
from util import print_progress_bar
import aiohttp
import asyncio


class ParserPapteki:
    URL = 'https://papteki.ru/'

    def __init__(self, max_request: int = 2):
        self.data = {'name': [], 'producer': [], 'cost': [], 'url': []}
        self.max_request = max_request
        self.all_address = set()
        self.categories = self._get_category_urls()
        self._pars_category()
        self._get_address()
        # asyncio.run(self._get_address_as())
        self._create_df()

    def _create_df(self) -> None:
        """Формирование структыры таблицы"""
        df = pd.DataFrame(self.data)
        if not ADDRESS:
            all_address = list(self.all_address)
        else:
            all_address = list(ADDRESS)
        all_address.sort()
        df['min'] = df[all_address].min(axis=1)
        df['max'] = df[all_address].max(axis=1)
        result = df[['name', 'producer', 'min', 'max'] + all_address]
        result.to_excel(f'result/Papteki.xlsx')

    async def _get_address_data(self, session: aiohttp.ClientSession, url: str, num: int):
        """Получение данных"""
        async with session.get(url) as result:
            result = await result.text()
            soup = BeautifulSoup(result, 'lxml')
            data = soup.find_all('div', class_='lk__table-row')
            for num_data, i in enumerate(data):
                if num_data:
                    result = [t.text.strip() for t in i.find_all('div', class_='lk__table-cell')]
                    address = ', '.join(result[0].split(',')[3:])
                    self.all_address.add(address)
                    self.data.setdefault(address, [None for _ in range(len(self.data['url']))])
                    self.data[address][num] = float(result[1].replace(' ', '').replace('руб.', '').replace('\xa0', ''))

    async def _get_address_as(self):
        """Асинхронное получение данных адресов"""
        print('Получение данных о наличии в аптеках')
        async with aiohttp.ClientSession() as session:
            pending = [self._get_address_data(session, i, num) for num, i in enumerate(self.data['url'])]

            progress = 0
            max_progress = len(self.data['url'])
            print_progress_bar(progress, max_progress, prefix='Progress:', suffix='Complete', length=50)

            while pending:
                sub_pending = [asyncio.create_task(i) for i in pending[:self.max_request]]
                del pending[:self.max_request]
                await asyncio.gather(*sub_pending)

                progress += len(sub_pending)
                print_progress_bar(progress, max_progress, prefix='Progress:', suffix='Complete', length=50)

    def _get_address(self) -> None:
        """Получение адресов аптек с наличием препарата"""
        print('Получение данных о наличии в аптеках')
        progress = 0
        max_progress = len(self.data['url'])
        print_progress_bar(progress, max_progress, prefix='Progress:', suffix='Complete', length=50)

        for num, i in enumerate(self.data['url']):
            html_text = requests.get(i).text
            soup = BeautifulSoup(html_text, 'lxml')
            data = soup.find_all('div', class_='lk__table-row')
            for num_data, j in enumerate(data):
                if num_data:
                    result = [t.text.strip() for t in j.find_all('div', class_='lk__table-cell')]
                    address = ', '.join(result[0].split(',')[3:])
                    self.all_address.add(address)
                    self.data.setdefault(address, [None for _ in range(len(self.data['url']))])
                    self.data[address][num] = float(result[1].replace(' ', '').replace('руб.', '').replace('\xa0', ''))

            progress += 1
            print_progress_bar(progress, max_progress, prefix='Progress:', suffix='Complete', length=50)

    def _get_category_urls(self) -> dict:
        """Получение данных о категориях. Возвращает словарь {name<str>: url<str>}"""
        html_text = requests.get(self.URL).text
        soup = BeautifulSoup(html_text, 'lxml')
        data = soup.find('ul', class_='pa-header__catalog-menu')
        links = data.find_all('a', class_='pa-header__catalog-link')
        menu = {}
        for i in links:
            menu[i.text.strip()] = i.attrs['href']
        return menu

    def _pars_category(self):
        """Ппарсинг категорий"""
        progress = 0
        max_progress = len(self.categories)
        print_progress_bar(progress, max_progress, prefix='Progress:', suffix='Complete', length=50)

        for name, url in self.categories.items():
            self._pars_product(name, url)

            progress += 1
            print_progress_bar(progress, max_progress, prefix='Progress:', suffix='Complete', length=50)

    def _pars_product(self, name, url, page=None, max_page=None) -> None:
        """Получает данные о всех товарах"""
        page_url = '' if page is None else f'?p={page}'
        html_text = requests.get(self.URL + url + page_url).text
        soup = BeautifulSoup(html_text, 'lxml')
        data = soup.find_all('div', class_='catalog__item')

        for i in data:
            obj = i.find('a', class_='catalog__item-title')
            self.data['name'].append(obj.text.strip() if obj else None)
            self.data['url'].append(self.URL + obj.attrs['href'][1:] if obj else None)
            producer_ = i.find('div', class_='catalog__item-name')
            self.data['producer'].append(producer_.text.replace('Производитель:', '') if producer_ else None)
            cost_ = i.find('div', class_='catalog__item-price').find('span')
            self.data['cost'].append(cost_.text if cost_ else None)

        if max_page is None:
            page_bl = soup.find('div', class_='pagination-bl')
            if page_bl is not None:
                max_page = int(page_bl.find_all('li', class_='page-item')[-2].find('a').text)
            else:
                max_page = 1
        page = 2 if page is None else page + 1
        if page <= max_page:
            self._pars_product(name, url, page, max_page)
