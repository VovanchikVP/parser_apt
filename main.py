import asyncio

import pandas as pd
from pathlib import Path
from parser_papteki import ParserPapteki, ParserAloeapteka
from lek import Parser24Lek, Parser24LekAsync
from typing import List
from lek.config import CITY
import os
import click
import ast
from lekopttorg.lekopttorg import ParserLekopttorg
from aptekanevis.aptekanevis import ParserAptekanevis
from acmespb.acmespb import ParserAcmespb


class PythonLiteralOption(click.Option):
    def type_cast_value(self, ctx, value):
        try:
            return ast.literal_eval(value)
        except Exception:
            raise click.BadParameter(value)


@click.group()
def parser():
    pass


@parser.command()
def papteki():
    ParserPapteki()


@parser.command()
def aloeapteka():
    ParserAloeapteka()


@parser.command()
def lekopttorg():
    asyncio.run(ParserLekopttorg().run_parser())


@parser.command()
def aptekanevis():
    asyncio.run(ParserAptekanevis().run_parser())


@parser.command()
def acmespb():
    asyncio.run(ParserAcmespb().run_parser())


@parser.command()
@click.argument('city')
@click.option("--pharmacies", cls=PythonLiteralOption)
def lek(city: int = 0, pharmacies: List[str] = None):
    city = int(city)
    result = os.path.dirname(__file__) / Path(f'lek_{CITY[city]}.csv')
    if os.path.exists(result):
        data = input('Найдены ранее полученные данные. Взять данные из них?(да/нет) по умолчанию (нет): ')
        if data == 'да':
            df = pd.read_csv(f'lek_{CITY[city]}.csv', index_col=0)
            Parser24LekAsync.create_excel_file(city, pharmacies, df)
        else:
            Parser24LekAsync(city, pharmacies)
    else:
        Parser24LekAsync(city, pharmacies)


if __name__ == '__main__':
    parser()
