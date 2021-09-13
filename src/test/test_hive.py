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
import os
import unittest
import pathlib
import sys
from shutil import copy2

from ensembl.production.core.config import parse_debug_var
from ensembl.production.core.models.hive import HiveInstance


here = pathlib.Path(__file__).parent.resolve()

LOG_LEVEL = logging.DEBUG if parse_debug_var(os.getenv("DEBUG")) else logging.WARNING

logging.basicConfig(
    stream=sys.stdout,
    format="%(asctime)s %(levelname)-8s %(name)s: %(message)s",
    level=LOG_LEVEL,
)

logger = logging.getLogger(__name__)
logging.getLogger('sqlalchemy.engine').setLevel(LOG_LEVEL)

DB_TEMPLATE_FILENAME = "test_pipeline.db.template"
DB_FILENAME = "test_pipeline.db.sqlite3"


class HiveTest(unittest.TestCase):
    """Create fresh database file"""

    def setUp(self):
        logger.info("Creating test sqlite database")
        copy2(here/DB_TEMPLATE_FILENAME, here/DB_FILENAME)
        logger.info("Connecting to hive test sqlite database %s", here/DB_FILENAME)
        self.hive = HiveInstance(f"sqlite:///{here/DB_FILENAME}")

    def test_create_job(self):
        """Basic test case for creating a new job"""
        job1 = self.hive.create_job('TestRunnable', {'x': 'y', 'a': 'b'})
        logger.debug(job1)
        job2 = self.hive.get_job_by_id(job1.job_id)
        logger.debug(job2)
        self.assertEqual(job1.job_id, job2.job_id)
        self.assertEqual(job1.analysis.logic_name, job2.analysis.logic_name)
        self.assertEqual(job1.input_id, job2.input_id)

    def test_check_semaphore_success(self):
        """Test case for checking on a finished semaphore"""
        semaphore_data = self.hive.get_semaphore_data(2)
        logger.debug(semaphore_data)
        status = self.hive.check_semaphores_for_job(semaphore_data)
        logger.debug("Status for 2 is " + status)
        self.assertEqual(status, 'complete', "Checking expected status for completed semaphore")

    def test_check_semaphore_incomplete(self):
        """Test case for checking on an incomplete semaphore"""
        semaphore_data = self.hive.get_semaphore_data(8)
        logger.debug("semaphore_data: %s", semaphore_data)
        status = self.hive.check_semaphores_for_job(semaphore_data)
        logger.debug("Status for 8 is " + status)
        self.assertEqual(status, 'incomplete', "Checking expected status for failed semaphore")

    def test_check_job_success(self):
        """Test case for checking on a completed single job"""
        job = self.hive.get_job_by_id(20)
        logger.debug(job)
        status = self.hive.get_job_tree_status(job)
        self.assertEqual("complete", status, "Checking status of completed single job")

    def test_check_job_failure(self):
        """Test case for checking on a failed single job"""
        job = self.hive.get_job_by_id(11)
        logger.debug(job)
        status = self.hive.get_job_tree_status(job)
        self.assertEqual("failed", status, "Checking status of failed single job")

    def test_check_job_tree_success(self):
        """Test case for checking on a completed job factory"""
        job = self.hive.get_job_by_id(1)
        logger.debug(job)
        status = self.hive.get_job_tree_status(job)
        logger.debug(status)
        self.assertEqual("complete", status, "Checking status of completed job factory")

    def test_check_job_tree_incomplete(self):
        """Test case for checking on an incomplete job factory"""
        job = self.hive.get_job_by_id(7)
        logger.debug("Job: %s", job)
        status = self.hive.get_job_tree_status(job)
        self.assertEqual("incomplete", status, "Checking status of incomplete job factory")

    def test_get_job_output_success(self):
        """Test case for getting output on a completed job factory"""
        output = self.hive.get_result_for_job_id(1)
        logger.debug(output)
        self.assertEqual('complete', output['status'], "Checking status of successful job factory output")
        self.assertTrue(output['output'] != None, "Checking output of successful job factory output")

    def test_get_job_output_incomplete(self):
        """Test case for getting output an incomplete job factory"""
        output = self.hive.get_result_for_job_id(7)
        logger.debug(output)
        self.assertEqual('incomplete', output['status'], "Checking status of incomplete job factory output")
        self.assertTrue('output' not in output, "Checking output of incomplete job factory output")

    def test_get_all_results(self):
        """Test case for listing all jobs"""
        jobs = self.hive.get_all_results('TestRunnable')
        self.assertEqual(1, len(jobs), "Checking we got just one job")

    def tearDown(self):
        """Remove test database file"""
        logger.info("Removing test sqlite database")
        os.remove(here/DB_FILENAME)


if __name__ == '__main__':
    unittest.main()
