#! /usr/bin/env python

"""
A Multi-threaded HTTP server
"""

import logging
import os
import re
import socket
import threading
from time import strftime, gmtime


class HTTP_Server():

    status_codes = {200: '200 OK', 404: '404 Not Found'}
    mime_types = {}
    default_mime_type = 'application/octet-stream'
    resource_dir = "www"
    response_status = "HTTP/1.1 {}\r\n"
    response_header_template = "Date: {}\r\nServer: {}\r\nLast-Modified: {}" +\
                               "\r\nContent-Type: {}\r\nContent-Length: {}" +\
                               "\r\n\n{}"
    rfc7231_date_template = "%a, %d %b %Y %H:%M:%S GMT"
    logging.basicConfig(level=logging.DEBUG)

    def __init__(self, host='', port=0):
        self.host = host
        self.port = port
        self.server_name = 'ArchServer'
        self.logger = logging.getLogger(__name__)
        self.file_access_count = {}
        self._set_mime_types()

    def listen(self, client_socket, client_ip):
            self.logger.info("Connection request from client %s" %
                             str(client_ip))
            msg = client_socket.recv(1024).decode('ascii')
            requested_file = re.search(r'GET /(.+) HTTP/1..*', msg).group(1)
            self.logger.debug("Requested file from client: %s" %
                              requested_file)
            reply = self._make_response(**{'requested_file': requested_file})
            self.logger.debug('Reply message : %s' % reply[:50])
            bytes_sent = client_socket.send(reply)
            print("Bytes sent: %s" % bytes_sent)
            client_socket.close()
            if self.status_codes[404] not in reply:
                print("/%s|%s|%d|%d" % (requested_file, client_ip[0],
                      client_ip[1], self.file_access_count[requested_file]))

    def _make_response(self, **kwargs):
        requested_file = kwargs['requested_file']
        file_path = os.path.join(self.resource_dir, requested_file)
        response = ""
        if os.path.exists(file_path):
            file_content = ""
            with open(file_path, 'rb') as fin:
                file_content = fin.read()
            response_code = self.status_codes[200]
            if requested_file in self.file_access_count:
                self.file_access_count[requested_file] += 1
            else:
                self.file_access_count[requested_file] = 1
        else:
            return self.response_status.format(self.status_codes[404])

        date = strftime(self.rfc7231_date_template, gmtime())
        modified_date = gmtime(os.path.getmtime(file_path))
        last_modified_date = strftime(self.rfc7231_date_template, modified_date)
        file_ext = requested_file.split('.')[-1]
        if file_ext in self.mime_types.keys():
            content_type = self.mime_types[file_ext]
        else:
            content_type = self.default_mime_type
        content_length = os.path.getsize(file_path)

        response = self.response_header_template.format(date, self.server_name,
                                                        last_modified_date,
                                                        content_type,
                                                        content_length,
                                                        file_content)
        return self.response_status.format(response_code) + response

    def _set_mime_types(self):
        with open('/etc/mime.types') as fin:
            for line in fin.readlines():
                if '\t' in line:
                    mime_type, ext_list = re.sub(r'\t+', '@', line).split('@')
                    for ext in ext_list.split():
                        self.mime_types[ext] = mime_type

    def run_server(self):
        try:
            server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            server_socket.bind((self.host, self.port))
            self.host = socket.gethostbyname(socket.gethostname())
            self.port = server_socket.getsockname()[1]
            self.logger.debug("Server bind at address %s:%s." %
                              (self.host, self.port))
            print("Host: %s, Port:%s" % (self.host, self.port))
            server_socket.listen(5)
            self.logger.info("Server listening at %s:%s" %
                             (self.host, self.port))
            while True:
                client_socket, client_ip = server_socket.accept()
                thread_args = (client_socket, client_ip)
                client_thread = threading.Thread(target=self.listen,
                                                 args=thread_args)
                client_thread.start()
        except Exception as e:
            print(e)
        finally:
            server_socket.close()
            self.logger.info("Server connection closed.")


if __name__ == '__main__':
    soc = HTTP_Server()
    soc.run_server()
