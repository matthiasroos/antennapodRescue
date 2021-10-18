import datetime
import xml.etree.ElementTree


def parse_pubdate(element: xml.etree.ElementTree.Element):
    """

    :return:
    """
    pubdate = element.find('pubDate').text

    try:
        datetime_format_1 = '%a, %d %b %Y %H:%M:%S %z'
        datetime_entry = datetime.datetime.strptime(pubdate, datetime_format_1).timestamp() * 1000
    except ValueError:
        datetime_format_2 = '%a, %d %b %Y %H:%M:%S %Z'
        datetime_entry = datetime.datetime.strptime(pubdate, datetime_format_2).timestamp() * 1000

    return datetime_entry
