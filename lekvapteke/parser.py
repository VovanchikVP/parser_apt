import asyncio
import pandas as pd

from bs4 import BeautifulSoup
import aiohttp
import json


class ParserLekvapteke:
    URL = 'https://lekvapteke.ru'
    PHARMACY = 'ЛекVаптеке'
    CLASSIFICATION = "classification"
    CATALOG = "catalog"
    PRODUCTS = "catalog__products-list"
    PRODUCT = "catalog__products-link"
    USE_FIELD = [
        'name', 'manufactory', 'price', 'url', 'apteka_name', 'city', 'raion', 'street', 'phone'
    ]

    def __init__(self, max_request: int = 1, city_id: int = 109):
        self.max_request = max_request
        self.city_id = city_id
        self.all_data = []
        self.finish_load_names = False

    async def run_parser(self):
        """Запуск парсера"""
        semaphore = asyncio.Semaphore(self.max_request)
        queue_drags = asyncio.Queue()
        async with aiohttp.ClientSession() as session:
            await session.get('https://lekvapteke.ru/')
            await session.get(f'https://lekvapteke.ru/ajax-set-redis-var/my_city/{self.city_id}')
            get_all = asyncio.create_task(self._get_all_drug_name(session, queue_drags))
            pr_name = asyncio.create_task(self._get_data(session, semaphore, queue_drags))
            await asyncio.gather(get_all, pr_name)
        await self._save_result()

    async def _get_data(self, session: aiohttp.ClientSession, semaphore: asyncio.Semaphore, queue_drags: asyncio.Queue):
        """Получение данных препаратов"""
        pending = set()
        while not self.finish_load_names or pending or not queue_drags.empty():
            while not queue_drags.empty():
                name = await queue_drags.get()
                pending.add(asyncio.create_task(self._get_drag_data(session, semaphore, name)))
            while pending:
                done, pending = await asyncio.wait(pending, return_when=asyncio.FIRST_COMPLETED)
                for done_task in done:
                    result = await done_task
                    print(result)
            await asyncio.sleep(1)

    async def _get_all_drug_name(self, session: aiohttp.ClientSession, queue_drags: asyncio.Queue):
        """Получение наименований всех препаратов"""
        async for class_url in self._get_classification_urls(session, self.CLASSIFICATION):
            print(class_url)
            await self._get_catalog_urls(session, class_url, self.CATALOG, queue_drags)
        self.finish_load_names = True

    async def _save_result(self) -> None:
        """Формирование структыры таблицы"""
        df = pd.DataFrame(self.all_data)
        df = df[self.USE_FIELD]
        writer = pd.ExcelWriter(f'result/{self.PHARMACY}.xlsx')
        for name in df['apteka_name'].unique():
            df_sheet = self._create_sheet(df.loc[df['apteka_name'] == name])
            df_sheet.to_excel(writer, sheet_name=name[:30])
        df.to_excel(writer, sheet_name='all')
        writer.close()

    @staticmethod
    def _create_sheet(data: pd.DataFrame):
        """Формирование листа с данными"""
        data = data.groupby(['name', 'manufactory', 'street'], as_index=False).first()
        data = data.pivot(index=['name', 'manufactory'], columns='street', values='price')
        data = data.astype(float)
        columns = [i for i in data.columns]
        max_ = data.max(axis=1, numeric_only=True)
        min_ = data.min(axis=1, numeric_only=True)
        data['min'] = min_
        data['max'] = max_
        columns_res = ['min', 'max']
        columns_res.extend(columns)
        return data[columns_res]

    async def _get_classification_urls(self, session: aiohttp.ClientSession, class_name: str):
        """Получение данных классификатора"""
        async with session.get(self.URL) as result:
            result = await result.text()
        soup = BeautifulSoup(result, 'lxml')
        data = soup.find('ul', class_=f'{class_name}__list')
        for link in data.find_all('a', class_=f'{class_name}__link'):
            yield f"{self.URL}{link.attrs['href']}"

    async def _get_catalog_urls(
            self, session: aiohttp.ClientSession, url: str, class_name: str, queue_drags: asyncio.Queue
    ):
        """Получение данных каталогов"""
        async with session.get(url) as result:
            result = await result.text()
        soup = BeautifulSoup(result, 'lxml')
        products = soup.find('ul', class_=self.PRODUCTS)
        if products:
            for link in products.find_all('a', class_=self.PRODUCT):
                queue_drags.put_nowait(link.attrs['href'].split("/")[-1])
        else:
            data = soup.find('ul', class_=f'{class_name}__list')
            if data:
                for link in data.find_all('a', class_=f'{class_name}__link'):
                    await self._get_catalog_urls(session, f"{self.URL}{link.attrs['href']}", class_name, queue_drags)

    async def _get_drag_data(self, session: aiohttp.ClientSession, semaphore: asyncio.Semaphore, name_drag: str):
        page = 0
        elements = 0
        item = True
        while item:
            async with semaphore:
                async with session.get(
                        f"https://lekvapteke.ru/ajax-get-pharmacies-with-medicament-shortname/{page}/{name_drag}/0"
                ) as result:
                    resp = await result.text()
                    try:
                        data = json.loads(resp)
                        if not data['pharmacies']:
                            item = False
                        else:
                            page += 100
                            elements += len(data['pharmacies'])
                            self.all_data.extend(data['pharmacies'])
                    except Exception:
                        print(resp)
                        return f"Ошибка {name_drag}!!!"

        return f"Загрузка {name_drag} ({elements} позиций) завершено!"
