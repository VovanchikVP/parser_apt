from parser_papteki import ParserPapteki, ParserAloeapteka
from lek.parser_24lek import Parser24Lek
import click


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
def lek(city):
    Parser24Lek(int(city))


if __name__ == '__main__':
    parser()
