#! /usr/bin/env python

"""
A Multi-threaded HTTP server
"""

import logging
import os
import re
import socket
import sys
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
    page = "<html>\n<title> Resource not found </title>\n<body>" +\
        "<h1>404 Not Found </h1> <p> Requested resource not available.<p>" +\
        "</body></html>"
    rfc7231_date_template = "%a, %d %b %Y %H:%M:%S GMT"
    logging.basicConfig(level=logging.ERROR)

    def __init__(self, host='', port=0):
        self.host = host
        self.port = port
        self.server_name = 'HTTP Server/Python 2.7'
        self.logger = logging.getLogger(__name__)
        self.file_access_count = {}
        # self._set_files()
        self._set_mime_types()

    def listen(self, client_socket, client_ip):
        """
        Serve the request to client
        :param client_socket: Socket object for client
        :param client_ip: IP address of client
        """
        self.logger.info("Connection request from client %s" %
                         str(client_ip))
        msg = client_socket.recv(1024).decode('ascii')

        parsed_request = re.search(r'GET /(.+) HTTP/1..*', msg)
        if parsed_request:
            requested_file = parsed_request.group(1)
            self.logger.debug("Requested file from client: %s" %
                              requested_file)
            reply = self._make_response(**{'requested_file': requested_file})
            self.logger.debug('Reply message : %s' % reply[:50])
            bytes_sent = client_socket.send(reply)
            self.logger.debug("Bytes sent: %s" % bytes_sent)
            client_socket.close()
            if self.status_codes[404] not in reply:
                # If file file found, check thread lock for file
                with self.file_access_count[requested_file][0]:
                    count = self.file_access_count[requested_file][1] + 1
                    self.file_access_count[requested_file][1] = count
                    print("/%s|%s|%d|%d" % (requested_file, client_ip[0],
                                            client_ip[1], count))
        else:
            self.logger.warning("Invalid request from client.")
            self.logger.warning(msg)

    def _make_response(self, **kwargs):
        """
        Create response header for requested file
        """
        requested_file = kwargs['requested_file']
        file_path = os.path.join(self.resource_dir, requested_file)
        response = ""
        file_content = ""
        if os.path.exists(file_path):
            with open(file_path, 'rb') as fin:
                file_content = fin.read()
            response_code = self.status_codes[200]
        else:
            error_response_header = "Content-Type: text/html\r\n" +\
                                    "Content-Length: {}\r\n\n{}"
            response = self.response_status.format(self.status_codes[404]) +\
                error_response_header.format(len(self.page), self.page)
            return response

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
        """
        Parse /etc/mime.types file for each file extension
        """
        with open('/etc/mime.types') as fin:
            for line in fin.readlines():
                if '\t' in line:
                    mime_type, ext_list = re.sub(r'\t+', '@', line).split('@')
                    for ext in ext_list.split():
                        self.mime_types[ext] = mime_type

    def _set_files(self):
        """
        Get info for all the files vailabel in resource directory.
        """
        if not os.path.exists(self.resource_dir):
            raise IOError('Resource directory "www" does not exists. Exiting..')

        for root, dirs, files in os.walk(self.resource_dir):
            for fname in files:
                f = os.path.join(root, fname).split("/", 1)[1]
                if f not in self.file_access_count.keys():
                    self.file_access_count[f] = [threading.Lock(), 0]

    def run_server(self):
        """
        Start the server on random port and assign thread to each client request.
        """
        try:
            threads = []
            server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            server_socket.bind((self.host, self.port))
            self.host = socket.gethostbyaddr(socket.gethostname())[0]
            self.port = server_socket.getsockname()[1]
            self.logger.debug("Server bind at address %s:%s." %
                              (self.host, self.port))
            print("Server is started on %s:%s" % (self.host, self.port))
            self._set_files()
            server_socket.listen(5)
            self.logger.info("Server listening at %s:%s" %
                             (self.host, self.port))
            while True:
                client_socket, client_ip = server_socket.accept()
                self._set_files()
                thread_args = (client_socket, client_ip)
                client_thread = threading.Thread(target=self.listen,
                                                 args=thread_args)
                threads.append(client_thread)
                client_thread.start()
        except IOError as e:
            raise e
        except Exception as e:
            self.logger.error(e)
        except KeyboardInterrupt:
            pass
        finally:
            for thread in threads:
                thread.join()
            server_socket.close()
            self.logger.info("Server connection closed.")


if __name__ == '__main__':
    try:
        soc = HTTP_Server()
        soc.run_server()
        sys.exit(0)
    except IOError as e:
        print(e)
        sys.exit(1)
    except Exception as e:
        soc.logger.error(e)
