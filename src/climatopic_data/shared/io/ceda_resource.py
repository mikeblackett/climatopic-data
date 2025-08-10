from collections.abc import Generator
from xml.etree import ElementTree

import dagster as dg
import requests
from urllib.parse import urljoin, urlparse

from upath import UPath


NAMESPACES = {
    'thredds': 'http://www.unidata.ucar.edu/namespaces/thredds/InvCatalog/v1.0',
}


class CEDAHTTPResource(dg.ConfigurableResource):
    base_url: str = 'https://dap.ceda.ac.uk/thredds/fileServer'
    access_token: str | None = None

    @property
    def headers(self) -> dict[str, str]:
        if self.access_token:
            return {'Authorization': f'Bearer {self.access_token}'}
        return {}

    def get_file_paths(self, url: str, extension: str = '.nc'):
        url = _to_xml_url(url)
        if not url.startswith('http'):
            url = urljoin(base=self.base_url, url=url)

        for url in _get_dataset_urls(url, extension=extension):
            yield UPath(url, headers=self.headers)


def _get_file_server_base(root: ElementTree.Element) -> str:
    for service in root.findall(
        path='.//thredds:service', namespaces=NAMESPACES
    ):
        if service.attrib.get('serviceType', '').lower() == 'httpserver':
            return service.attrib['base']
    raise RuntimeError('No fileServer (HTTPServer) service found')


def _get_dataset_urls(
    url: str, extension: str = '.nc'
) -> Generator[str, None, None]:
    response = requests.get(url)
    response.raise_for_status()

    root = ElementTree.fromstring(response.content)

    file_server_base = _get_file_server_base(root)

    for dataset in root.findall(
        path='.//thredds:dataset', namespaces=NAMESPACES
    ):
        url_path = dataset.attrib.get('urlPath')
        if url_path and url_path.lower().endswith(extension):
            yield urljoin(base=url, url=file_server_base + url_path)


def _to_xml_url(url: str) -> str:
    obj = urlparse(url)
    path = obj.path
    if path.endswith('.html'):
        path = path.replace('.html', '.xml')
    if not path.endswith('.xml'):
        path = path + '.xml'
    return obj._replace(path=path).geturl()
