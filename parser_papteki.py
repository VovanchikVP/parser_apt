import requests
from bs4 import BeautifulSoup
import pandas as pd

from config import REGIONS_ALOEAPTEKA


class ParserPapteki:
    URL = 'https://papteki.ru/'

    def __init__(self):
        self.data = {'name': [], 'producer': [], 'cost': [], 'url': []}
        self.categories = self._get_category_urls()
        self._pars_category()
        df = pd.DataFrame(self.data)
        df.to_excel('Papteki.xlsx')

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
        for name, url in self.categories.items():
            self._pars_product(name, url)

    def _pars_product(self, name, url, page=None, max_page=None) -> None:
        """Получает данные о всех товарах"""
        page_url = '' if page is None else f'?p={page}'
        html_text = requests.get(self.URL + url + page_url).text
        soup = BeautifulSoup(html_text, 'lxml')
        data = soup.find_all('div', class_='catalog__item')

        for i in data:
            obj = i.find('a', class_='catalog__item-title')
            self.data['name'].append(obj.text.strip())
            self.data['url'].append(self.URL + obj.attrs['href'][1:])
            self.data['producer'].append(i.find('div', class_='catalog__item-name').text.replace('Производитель:', ''))
            self.data['cost'].append(i.find('div', class_='catalog__item-price').find('span').text)

        if max_page is None:
            page_bl = soup.find('div', class_='pagination-bl')
            if page_bl is not None:
                max_page = int(page_bl.find_all('li', class_='page-item')[-2].find('a').text)
            else:
                max_page = 1
        page = 2 if page is None else page + 1
        print(f'Категория: {name}.', f'Завершено страниц {page - 1} из {max_page}')
        if page <= max_page:
            self._pars_product(name, url, page, max_page)


class ParserAloeapteka:
    URL = 'https://aloeapteka.ru/'

    def __init__(self):
        self.data = {'name': [], 'producer': [], 'cost': [], 'url': []}
        self.cookies = {}
        for city_name, city_id in REGIONS_ALOEAPTEKA.items():
            print(f'Прсинг {city_name} начат.')
            self.cookies = self._create_cookies(city_id)
            self.categories = self._get_category_urls()
            print(f'Категории собраны: {len(self.categories)} шт.')
            self.sub_categories = self._get_sub_category()
            print(f'Cубкатегории собраны: {len(self.sub_categories)} шт.')
            self._pars_category()
            df = pd.DataFrame(self.data)
            df.to_excel(f'Aloeapteka_{city_name}.xlsx')
            self.data = {'name': [], 'producer': [], 'cost': [], 'url': []}
            print(f'Прсинг {city_name} завершен.')

    def _get_category_urls(self) -> dict:
        """Получение данных категорий"""
        html_text = requests.get(self.URL, cookies=self.cookies).text
        soup = BeautifulSoup(html_text, 'lxml')
        data = soup.find('nav', class_='main-nav d-flex justify-content-between')
        links = data.find_all('a', class_='')
        menu = {}
        for i in links:
            menu[i.text.strip()] = i.attrs['href']
        return menu

    def _get_sub_category(self):
        """Ппарсинг подкатегорий категорий"""
        menu = {}
        for name, url in self.categories.items():
            html_text = requests.get(self.URL + url[1:], cookies=self.cookies).text
            soup = BeautifulSoup(html_text, 'lxml')
            data = soup.find('section', class_='group-catalog')
            links = data.find_all('a')
            for i in links:
                menu[i.text.strip()] = i.attrs['href']
        return menu

    def _pars_category(self):
        """Ппарсинг категорий"""
        for name, url in self.sub_categories.items():
            self._pars_product(name, url)

    def _pars_product(self, name, url, page=None, max_page=None) -> None:
        """Получает данные о всех товарах"""
        page_url = '' if page is None else f'?page_num={page}'
        html_text = requests.get(self.URL + url[1:] + page_url, cookies=self.cookies).text
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
        print(f'Категория: {name}.', f'Завершено страниц {page - 1} из {max_page}')
        if page <= max_page:
            self._pars_product(name, url, page, max_page)

    @staticmethod
    def _create_cookies(city_id: str) -> dict:
        """Формирование cookies"""
        return {'city_id': city_id}
