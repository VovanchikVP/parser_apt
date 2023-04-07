from parser_papteki import ParserPapteki, ParserAloeapteka
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


if __name__ == '__main__':
    parser()
