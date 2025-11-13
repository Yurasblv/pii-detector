from datetime import datetime

__version__ = '0.0.1'
__build_date__ = ''


def description() -> str:
    if __build_date__:
        date = datetime.strptime(__build_date__, '%Y-%m-%dT%H:%M:%SZ')
    else:
        date = datetime.now()
    return 'Last updated on: ' + date.strftime('%b %d %Y') + ' at ' + date.strftime('%H:%M:%S')


def version() -> str:
    return __version__


def build_date() -> str:
    return __build_date__
