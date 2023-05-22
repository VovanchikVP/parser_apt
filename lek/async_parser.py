import json
import aiohttp
import asyncio
import pandas as pd
from typing import List
from copy import deepcopy
from .config import CITY
from .parser_24lek import Parser24Lek
from util import print_progress_bar


class Parser24LekAsync(Parser24Lek):

    def __init__(self, city: int = 0, pharmacies: List[str] = None):
        self.pharmacies = pharmacies
        self.city = city
        if city not in CITY:
            raise ValueError('Не верный код города')
        with open(self.PREPARATS) as f:
            self.preparats = json.load(f)
        self.result = {'name': [], 'cost': [], 'address': [], 'org_name': []}
        self.get_params = deepcopy(self.GET_PARAMS)
        self.get_params['city'] = city
        asyncio.run(self._run())
        self.df = pd.DataFrame(self.result)
        self.create_excel_file(city, pharmacies, self.df)
        self.df.to_csv(f'lek_{CITY[city]}.csv')

    async def _run(self):
        pending = []
        async with aiohttp.ClientSession() as session:
            for num, preparat in enumerate(self.preparats):
                params = deepcopy(self.get_params)
                params['query'] = preparat
                pending.append(asyncio.create_task(self._fetch(session, params)))

            progress = 0
            max_progress = len(pending)
            print_progress_bar(progress, max_progress, prefix='Progress:', suffix='Complete', length=50)

            while pending:
                done, pending = await asyncio.wait(
                    pending, return_when=asyncio.FIRST_COMPLETED)

                for done_task in done:
                    for feature in await done_task:
                        self._prepare_feature(feature['properties'])
                progress += len(done)
                print_progress_bar(progress, max_progress, prefix='Progress:', suffix='Complete', length=50)

    @classmethod
    async def _fetch(cls, session: aiohttp.ClientSession, params: dict) -> int:
        async with session.get(cls.URL, params=params) as result:
            result = await result.text()
            result = json.loads(result.lstrip('\ufeff'))
            return result['features']
