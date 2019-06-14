import requests
from urllib.parse import urljoin


class Client:
    def __init__(self, host):
        self.host = host

    def get(self, path, data=None, raw_response_content=False):
        """
        GET API endpoint
        :param path: API endpoint
        :param data: query parameters
        :param raw_response_content: return raw response content instead of parsing as JSON to dict
        :return: dic object or content of response in bytes
        """
        response = requests.get(self.url(path), params=data)
        return response.content if raw_response_content else response.json()

    def patch(self, path, data, raw_response_content=False):
        """
        PATCH API endpoint
        :param path: API endpoint
        :param data: data to send as body
        :param raw_response_content: return raw response content instead of parsing as JSON to dict
        :return: dic object or content of response in bytes
        """
        response = requests.patch(self.url(path), data=data)
        return response.content if raw_response_content else response.json()

    def post(self, path, data=None, raw_response_content=False):
        """
        POST API endpoint
        :param path: API endpoint
        :param data: multipart/form-data parameters
        :param raw_response_content: return raw response content instead of parsing as JSON to dict
        :return: dic object or content of response in bytes
        """
        response = requests.post(self.url(path), files=data)
        return response.content if raw_response_content else response.json()

    def url(self, path):
        """
        Joint host address with API endpoint
        :param path: API endpoint
        :return: URL
        """
        return urljoin(self.host, path)
