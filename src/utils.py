import datetime
import xml.etree.ElementTree


def parse_pubdate(element: xml.etree.ElementTree.Element):
    """

    >>> item = xml.etree.ElementTree.Element('item')
    ... f = xml.etree.ElementTree.SubElement(item, 'pubDate')
    ... f.text = 'Sun, 17 Oct 2021 11:00:32 GMT'
    ... parse_pubdate(element=item)

    1634461232000


    >>> item = xml.etree.ElementTree.Element('item')
    ... f = xml.etree.ElementTree.SubElement(item, 'pubDate')
    ... f.text = 'Sun, 17 Oct 2021 11:00:32 +0200'
    ... parse_pubdate(element=item)

    1634461232000

    :return:
    """
    pubdate = element.find('pubDate').text

    try:
        datetime_format_1 = '%a, %d %b %Y %H:%M:%S %z'
        datetime_entry = datetime.datetime.strptime(pubdate, datetime_format_1).timestamp() * 1000
    except ValueError:
        datetime_format_2 = '%a, %d %b %Y %H:%M:%S %Z'
        datetime_entry = datetime.datetime.strptime(pubdate, datetime_format_2).timestamp() * 1000

    return int(datetime_entry)
