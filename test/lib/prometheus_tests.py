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


class EndpointMode(Enum):
    RETURN_VALID_RESPONSE = auto()
    RETURN_INVALID_RESPONSE = auto()


class TestMetricsHTTPHandler(http.server.BaseHTTPRequestHandler):
    """A test HTTP endpoint for Prometheus to scrape."""

    mode: EndpointMode

    def __init__(self, mode: EndpointMode, *args) -> None:
        self.mode = mode
        super().__init__(*args)

    def do_GET(self):
        if self.path != '/metrics':
            self.send_error(404)

        self.send_response(200)
        self.end_headers()

        if self.mode == EndpointMode.RETURN_VALID_RESPONSE:
            self.wfile.write(b'# TYPE test_family gauge\n'
                             b'test_family 123\n')

        elif self.mode == EndpointMode.RETURN_INVALID_RESPONSE:
            self.wfile.write(b'# TYPE test_family gauge\n'
                             b'test_family123\n')

        else:
            raise NotImplementedError(f'unknown mode {self.mode}')




# class TargetScrapeStatus(typing.NamedTuple):
#     health: str
#     lastError: str
#
#
# TargetsScrapeHistory = Dict[str, Dict[str, TargetScrapeStatus]]
#
#
# def collect_target_scrape_history(min_scrapes: int = 5) -> TargetsScrapeHistory:
#     target_histories = defaultdict(dict)
#
#     while True:
#         targets = prometheus.api.get_targets()
#         print(targets)
#
#         for target in targets['activeTargets']:
#             labels = frozendict(target['labels'])
#
#             history = target_histories[labels]
#
#             if target['health'] == 'unknown':
#                 # hasn't been scraped yet
#                 continue
#
#             ts = target['lastScrape']
#             history[ts] = TargetScrapeStatus(target['health'], target['lastError'])
#
#         # collect min_scrapes or more scrape statuses for each target
#         if len(target_histories) > 0 and all([len(v) >= min_scrapes for v in target_histories.values()]):
#             break
#
#         time.sleep(1)
#
#     return target_histories
#
#
# def is_target_healthy(target: str, scrape_history: TargetsScrapeHistory) -> bool:
#     target_history = scrape_history[target]
#
#     return len(target_history) and all([h.health == 'up' for h in target_history.values()])



# assert run_test(EndpointMode.RETURN_VALID_RESPONSE) is True
# assert run_test(EndpointMode.RETURN_INVALID_RESPONSE) is False


class TestMetricsHandlerTest(unittest.TestCase):
    def test(self):
        cm = contextlib.ExitStack()

        work_dir = Path(cm.enter_context(tempfile.TemporaryDirectory()))

        archive = RemotePrometheusArchive.for_tag('latest').download()
        prometheus: PrometheusInstance = cm.enter_context(PrometheusInstance(archive, work_dir))

        prometheus.start()


        def run_test(mode: EndpointMode):
            httpd = http.server.HTTPServer(ENDPOINT_ADDRESS, partial(TestMetricsHTTPHandler, mode))
            thread = threading.Thread(target=httpd.serve_forever, daemon=True)

            thread.start()

            try:
                pass
                # prometheus.set_static_scrape_config('test', [ENDPOINT_ADDRESS])
                #
                # history = collect_target_scrape_history()
                # print(history)
                # return is_target_healthy('test', history)

            finally:
                httpd.shutdown()
                thread.join()


class ConcurrentPrometheusInstancesTest(unittest.TestCase):
    def test_concurrent_instances(self):
        """verify that trying to start a 2nd copy of prometheus fails.
            prometheus
            this is handled by creating a unique server tls cert for each instance and requiring a valid cert on connections.
            if the api client connects to the wrong instance cert verification will fail and  """
        cm = contextlib.ExitStack()  # TODO: clean this up

        work_dir1 = Path(cm.enter_context(tempfile.TemporaryDirectory()))  # TODO:  make these delete only if no exception occured
        work_dir2 = Path(cm.enter_context(tempfile.TemporaryDirectory()))

        archive = RemotePrometheusArchive.for_tag('latest').download()
        prometheus1: PrometheusInstance = cm.enter_context(PrometheusInstance(archive, work_dir1))
        prometheus2: PrometheusInstance = cm.enter_context(PrometheusInstance(archive, work_dir2))

        prometheus1.start()

        with self.assertRaisesRegex(Exception, 'certificate verify failed'):
            prometheus2.start()


        cm.close()

