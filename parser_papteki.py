import requests
from bs4 import BeautifulSoup
import pandas as pd


class ParserPapteki:
    URL = 'https://papteki.ru/'

    def __init__(self):
        self.data = {'name': [], 'producer': [], 'cost': [], 'url': []}
        self.categories = self._get_category_urls()
        self.pars_category()
        df = pd.DataFrame(self.data)
        df.to_excel('test.xlsx')

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

    def pars_category(self):
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
