import copy
import time

import requests
from bs4 import BeautifulSoup
import pandas as pd
from util import print_progress_bar

from aloeapteka.config import REGIONS_ALOEAPTEKA, ADDRESS


class ParserAloeapteka:
    URL = 'https://aloeapteka.ru/'
    URL_ADDRESS = 'https://aloeapteka.ru/ajax/get_item_availabil.php'
    HEADERS = {
        "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
        "Accept-Encoding": "gzip, deflate, br",
        "Accept": "application/json, text/javascript, */*; q=0.01",
        "Referer": None
    }

    def __init__(self):
        self.data = {'name': [], 'producer': [], 'cost': [], 'url': [], 'address': []}
        self.cookies = {}
        self.all_address = set()
        self.session = requests.session()
        t_start = time.time()
        for city_name, city_id in REGIONS_ALOEAPTEKA.items():
            print(f'Прсинг {city_name} начат.')
            self.cookies = self._create_cookies(city_id)
            self.categories = self._get_category_urls()
            print(f'Категории собраны: {len(self.categories)} шт.')
            self.sub_categories = self._get_sub_category()
            print(f'Cубкатегории собраны: {len(self.sub_categories)} шт.')
            self._pars_category()
            self._get_address()
            self._create_df(city_name)
            self.data = {'name': [], 'producer': [], 'cost': [], 'url': [], 'address': []}
            print(f'Прсинг {city_name} завершен. Затраченное время: {time.time() - t_start} секунд.')

    def _get_category_urls(self) -> dict:
        """Получение данных категорий"""
        resp = self.session.get(self.URL, cookies=self.cookies)
        html_text = resp.text
        soup = BeautifulSoup(html_text, 'lxml')
        data = soup.find('nav', class_='main-nav d-flex justify-content-between')
        links = data.find_all('a', class_='')
        menu = {}
        for i in links:
            menu[i.text.strip()] = i.attrs['href']
        return menu

    def _create_df(self, city_name: str) -> None:
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
        result.to_excel(f'result/Aloeapteka_{city_name}.xlsx')

    def _get_address(self) -> None:
        """Получение адресов аптек с наличием препарата"""
        print('Получение данных о наличии в аптеках')
        progress = 0
        max_progress = len(self.data['url'])
        print_progress_bar(progress, max_progress, prefix='Progress:', suffix='Complete', length=50)

        for i in self.data['url']:
            headers = copy.copy(self.HEADERS)
            headers['Referer'] = i
            prep_id = int(i.split('-')[-1].replace('/', ''))
            result = self.session.post(
                self.URL_ADDRESS,
                data={'id': prep_id},
                cookies=self.session.cookies,
                headers=headers
            ).json()
            if result['coords']:
                address = list(result['coords'].keys())
            else:
                address = []
            self.all_address.update(address)
            self.data['address'].append(address)

            progress += 1
            print_progress_bar(progress, max_progress, prefix='Progress:', suffix='Complete', length=50)

    def _get_sub_category(self):
        """Ппарсинг подкатегорий категорий"""
        menu = {}
        for name, url in self.categories.items():
            resp = self.session.get(self.URL + url[1:], cookies=self.session.cookies)
            html_text = resp.text
            soup = BeautifulSoup(html_text, 'lxml')
            data = soup.find('section', class_='group-catalog')
            links = data.find_all('a')
            for i in links:
                menu[i.text.strip()] = i.attrs['href']
        return menu

    def _pars_category(self):
        """Ппарсинг категорий"""
        progress = 0
        max_progress = len(self.sub_categories)
        print_progress_bar(progress, max_progress, prefix='Progress:', suffix='Complete', length=50)

        for name, url in self.sub_categories.items():
            self._pars_product(name, url)

            progress += 1
            print_progress_bar(progress, max_progress, prefix='Progress:', suffix='Complete', length=50)

    def _pars_product(self, name, url, page=None, max_page=None) -> None:
        """Получает данные о всех товарах"""
        page_url = '' if page is None else f'?page_num={page}'
        html_text = self.session.get(self.URL + url[1:] + page_url, cookies=self.session.cookies).text
        soup = BeautifulSoup(html_text, 'lxml')
        data = soup.find_all('div', class_='goods-list__item specialitem0 goods-list__item')

        for i in data:
            obj_name = i.find('div', class_='swiper-slide__info')
            name_ = obj_name.find('a')
            self.data['name'].append(name_.text.strip() if name_ else name_)
            url_ = obj_name.find('a')
            self.data['url'].append(self.URL + url_.attrs['href'][1:] if url_ else None)
            producer_ = obj_name.find('small')
            self.data['producer'].append(producer_.text.strip() if producer_ else None)
            cost_ = i.find('div', class_='btns-prices')
            self.data['cost'].append(cost_.text if cost_ else None)

        if max_page is None:
            page_bl = soup.find('ul', class_='pagination justify-content-center')
            if page_bl is not None:
                max_page = int(page_bl.find_all('li', class_='page-item')[-2].find('a').text)
            else:
                max_page = 1
        page = 2 if page is None else page + 1
        if page <= max_page:
            self._pars_product(name, url, page, max_page)

    @staticmethod
    def _create_cookies(city_id: str) -> dict:
        """Формирование cookies"""
        return {'city_id': city_id}
