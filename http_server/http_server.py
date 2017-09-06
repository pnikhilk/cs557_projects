#! /usr/bin/env python

"""
A Multi-threaded HTTP server
"""

import re


class HTTP_Server():

    status_codes = {200: 'OK', 404: 'Not Found'}
    mime_types = {}
    default_mime_type = 'application/octet-stream'

    def __init__(self, host='', port=''):
        self.host = host
        self.port = port
        self.server_name = 'ArchServer'
        self._set_mime_types()

    def listen(self):
        raise NotImplemented

    def _make_response(self, **kwargs):
        raise NotImplemented

    def _set_mime_types(self):
        with open('/etc/mime.types') as fin:
            for line in fin.readline():
                mime_type, ext = re.sub(r'\t+', ' ', line).split()
                if ext:
                    self.mime_types[ext] = mime_type

    def run_server(self):
        raise NotImplemented
