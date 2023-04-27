import json
import os
import requests
import pandas as pd
from bs4 import BeautifulSoup
from pathlib import Path
from copy import deepcopy
from .config import CITY


class Parser24Lek:
    URL = 'https://24lek.ru/data.json.php'
    GET_PARAMS = {'query': '', 'city': 0, 'raon': 0, 'apteka': 0, 'remove_content': 0}
    PREPARATS = os.path.dirname(__file__) / Path('preparats.json')

    def __init__(self, city: int = 0):
        if city not in CITY:
            raise ValueError('Не верный код города')
        with open(self.PREPARATS) as f:
            self.preparats = json.load(f)
        self.result = {'name': [], 'cost': [], 'address': [], 'org_name': []}
        self.get_params = deepcopy(self.GET_PARAMS)
        self.get_params['city'] = city
        self._run()
        df = pd.DataFrame(self.result)
        df.to_excel(f'lek_{CITY[city]}.xlsx')

    def _run(self):
        for num, preparat in enumerate(self.preparats):
            self.get_params['query'] = preparat
            req = requests.get(url=self.URL, params=self.get_params)
            req = json.loads(req.text.lstrip('\ufeff'))
            for feature in req['features']:
                self._prepare_feature(feature['properties'])
            print(f"Прогресс: {num + 1}:{len(self.preparats)}")

    def _prepare_feature(self, feature):
        soup_org_name = BeautifulSoup(feature['balloonContentHeader'], 'lxml')
        org_name = soup_org_name.find('div').text.strip()
        soup_preparats_data = BeautifulSoup(feature['balloonContentBody'], 'lxml')
        address = soup_preparats_data.find('a').text.strip()
        preparats_name = [i.text.strip() for i in soup_preparats_data.find_all('span')]
        preparats_cost = [i.text.strip() for i in soup_preparats_data.find_all('strong')]
        self.result['name'].extend(preparats_name)
        self.result['cost'].extend(preparats_cost)
        self.result['address'].extend([address for _ in range(len(preparats_name))])
        self.result['org_name'].extend([org_name for _ in range(len(preparats_name))])
