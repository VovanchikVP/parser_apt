import pandas as pd
from pathlib import Path
from parser_papteki import ParserPapteki, ParserAloeapteka
from lek.parser_24lek import Parser24Lek
from typing import List
from lek.config import CITY
import os
import click
import ast


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
@click.argument('city')
@click.option("--pharmacies", cls=PythonLiteralOption)
def lek(city: int = 0, pharmacies: List[str] = None):
    city = int(city)
    result = os.path.dirname(__file__) / Path(f'lek_{CITY[city]}.csv')
    if os.path.exists(result):
        data = input('Найдены ранее полученные данные. Взять данные из них?(да/нет) по умолчанию (нет): ')
        if data == 'да':
            df = pd.read_csv(f'lek_{CITY[city]}.csv', index_col=0)
            Parser24Lek.create_excel_file(city, pharmacies, df)
        else:
            Parser24Lek(city, pharmacies)
    else:
        Parser24Lek(city, pharmacies)


if __name__ == '__main__':
    parser()
