import contextlib
import http.server
import logging
import random
import socketserver
import tempfile
import threading
import time
import typing
import unittest
from collections import defaultdict
from datetime import datetime
from enum import Enum, auto
from functools import partial
from pathlib import Path
from typing import Dict

from frozendict import frozendict

from lib.net import SocketAddress
from lib.prometheus import PrometheusInstance, RemotePrometheusArchive


logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(f'{__name__}')


ENDPOINT_ADDRESS = SocketAddress('localhost', 9500)


class TestMetricsHTTPHandler(http.server.BaseHTTPRequestHandler):
    """A test HTTP endpoint for Prometheus to scrape."""


    def do_GET(self):
        if self.path != '/metrics':
            self.send_error(404)

        self.send_response(200)
        self.end_headers()

        self.wfile.write(b"""
# TYPE test_counter counter
test_counter {abc="123"} 0
test_counter {abc="456"} 0

test_untyped {abc="123"} 0
test_untyped {abc="456"} 0
""")


cm = contextlib.ExitStack()

work_dir = Path(cm.enter_context(tempfile.TemporaryDirectory()))

archive = RemotePrometheusArchive.for_tag('latest').download()
prometheus: PrometheusInstance = cm.enter_context(PrometheusInstance(archive, work_dir))

prometheus.start()



httpd = http.server.HTTPServer(ENDPOINT_ADDRESS, TestMetricsHTTPHandler)
thread = threading.Thread(target=httpd.serve_forever, daemon=True)

prometheus.set_static_scrape_config('test', [ENDPOINT_ADDRESS])

thread.start()

input('Press any key...')


httpd.shutdown()
thread.join()


cm.close()


