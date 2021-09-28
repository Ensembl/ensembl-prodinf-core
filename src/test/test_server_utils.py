#    See the NOTICE file distributed with this work for additional information
#    regarding copyright ownership.
#    Licensed under the Apache License, Version 2.0 (the "License");
#    you may not use this file except in compliance with the License.
#    You may obtain a copy of the License at
#        http://www.apache.org/licenses/LICENSE-2.0
#    Unless required by applicable law or agreed to in writing, software
#    distributed under the License is distributed on an "AS IS" BASIS,
#    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#    See the License for the specific language governing permissions and
#    limitations under the License.


import logging
import unittest
import os
import sys

from ensembl.production.core.config import parse_debug_var
import ensembl.production.core.server_utils as su

LOG_LEVEL = logging.DEBUG if parse_debug_var(os.getenv("DEBUG")) else logging.WARNING

logging.basicConfig(
    stream=sys.stdout,
    format="%(asctime)s %(levelname)-8s %(name)s: %(message)s",
    level=LOG_LEVEL,
)
logger = logging.getLogger(__name__)

class ServerTest(unittest.TestCase):
    def test_status(self):
        status = su.get_status()
        logger.debug(status)
        self.assertTrue(status["n_cpus"] >= 1)
        self.assertTrue(status["load_1m"] >= 0)
        self.assertTrue(status["load_5m"] >= 0)
        self.assertTrue(status["load_15m"] >= 0)
        self.assertTrue(status["memory_total_m"] >= 0)
        self.assertTrue(status["memory_used_m"] >= 0)
        self.assertTrue(status["memory_available_m"] >= 0)
        self.assertTrue(status["memory_used_pct"] >= 0)
        self.assertTrue(status["memory_used_pct"] <= 100)

    def test_status_dir(self):
        status = su.get_status(dir_name="/")
        logger.debug(status)
        self.assertTrue(status["n_cpus"] >= 1)
        self.assertTrue(status["load_1m"] >= 0)
        self.assertTrue(status["load_5m"] >= 0)
        self.assertTrue(status["load_15m"] >= 0)
        self.assertTrue(status["memory_total_m"] >= 0)
        self.assertTrue(status["memory_used_m"] >= 0)
        self.assertTrue(status["memory_available_m"] >= 0)
        self.assertTrue(status["memory_used_pct"] >= 0)
        self.assertTrue(status["memory_used_pct"] <= 100)
        self.assertTrue(status["disk_total_g"] >= 0)
        self.assertTrue(status["disk_used_g"] >= 0)
        self.assertTrue(status["disk_available_g"] >= 0)
        self.assertTrue(status["disk_used_pct"] >= 0)
        self.assertTrue(status["disk_used_pct"] <= 100)


class AssertTest(unittest.TestCase):
    def test_raises_assert_http_uri(self):
        self.assertRaises(ValueError, su.assert_http_uri, '')
        self.assertRaises(ValueError, su.assert_http_uri, 'invalid_uri')
        self.assertRaises(ValueError, su.assert_http_uri, 'http://uri-with-no-slash')
        self.assertRaises(ValueError, su.assert_http_uri, 'mysql://wrong-schema')

    def test_passes_assert_http_uri(self):
        su.assert_http_uri('http://server-name/')
        su.assert_http_uri('https://server-name:port/')

    def test_raises_assert_mysql_uri(self):
        self.assertRaises(ValueError, su.assert_mysql_uri, '')
        self.assertRaises(ValueError, su.assert_mysql_uri, 'invalid_uri')
        self.assertRaises(ValueError, su.assert_mysql_uri, 'http://wrong_schema')
        self.assertRaises(ValueError, su.assert_mysql_uri, 'mysql://invalid')
        self.assertRaises(ValueError, su.assert_mysql_uri, 'mysql://user@server-no-slash')
        self.assertRaises(ValueError, su.assert_mysql_uri, 'mysql://user:pass@server-no-port/')

    def test_passes_assert_mysql_uri(self):
        su.assert_mysql_uri('mysql://user@server:3006/')
        su.assert_mysql_uri('mysql://user:pass@server:3306/')

    def test_raises_assert_mysql_db_uri(self):
        self.assertRaises(ValueError, su.assert_mysql_db_uri, '')
        self.assertRaises(ValueError, su.assert_mysql_db_uri, 'invalid_uri')
        self.assertRaises(ValueError, su.assert_mysql_db_uri, 'http://wrong_schema')
        self.assertRaises(ValueError, su.assert_mysql_db_uri, 'mysql://invalid')
        self.assertRaises(ValueError, su.assert_mysql_db_uri, 'mysql://user:pass@server-no-slash')
        self.assertRaises(ValueError, su.assert_mysql_db_uri, 'mysql://user:pass@server-no-db:3306/')

    def test_pass_assert_mysql_db_uri(self):
        su.assert_mysql_db_uri('mysql://user:pass@server:3306/db_name')

    def test_raises_assert_email(self):
        self.assertRaises(ValueError, su.assert_email, '')
        self.assertRaises(ValueError, su.assert_email, 'invalid_email.com')

    def test_passes_assert_email(self):
        su.assert_email('valid.email@domain.ac.uk')
