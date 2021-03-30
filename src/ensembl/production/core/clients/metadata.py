# See the NOTICE file distributed with this work for additional information
#   regarding copyright ownership.
#   Licensed under the Apache License, Version 2.0 (the "License");
#   you may not use this file except in compliance with the License.
#   You may obtain a copy of the License at
#       http://www.apache.org/licenses/LICENSE-2.0
#   Unless required by applicable law or agreed to in writing, software
#   distributed under the License is distributed on an "AS IS" BASIS,
#   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#   See the License for the specific language governing permissions and
#   limitations under the License.
import logging

from ensembl.production.core.rest import RestClient
from ensembl.production.core.server_utils import assert_mysql_db_uri


class MetadataClient(RestClient):

    def submit_job(self, database_uri, e_release, eg_release, release_date, current_release, email, comment, source):
        assert_mysql_db_uri(database_uri)

        payload = {
            'database_uri': database_uri,
            'e_release': e_release,
            'eg_release': eg_release,
            'release_date': release_date,
            'current_release': current_release,
            'email': email,
            'comment': comment,
            'source': source
        }
        return super(MetadataClient, self).submit_job(payload)

    def print_job(self, job, print_results=False, print_input=False):
        logging.info("Job %s (%s) to (%s) - %s" % (
            job['id'], job['input']['metadata_uri'], job['input']['database_uri'], job['status']))
        if print_input == True:
            self.print_inputs(job['input'])
        if job['status'] == 'complete':
            if print_results == True:
                logging.info("Load result: " + str(job['status']))
                logging.info("Load took: " + str(job['output']['runtime']))
        elif job['status'] == 'running':
            if print_results == True:
                logging.info("Load result: " + str(job['status']))
                logging.info(str(job['progress']['complete']) + "/" + str(job['progress']['total']) + " task complete")
                logging.info("Status: " + str(job['progress']['message']))
        elif job['status'] == 'failed':
            failure_msg = self.retrieve_job_failure(job['id'])
            logging.info("Job failed with error: " + str(failure_msg['msg']))

    def print_inputs(self, i):
        logging.info("database URI: " + i['database_uri'])
        logging.info("Ensembl release number: " + i['e_release'])
        logging.info("Release date: " + i['release_date'])
        logging.info("Is it the current release: " + i['current_release'])
        if 'eg_release' in i:
            logging.info("EG release number: " + i['eg_release'])
            logging.info("Email of submitter: " + i['email'])
            logging.info("Comment: " + i['comment'])
            logging.info("Source: " + i['source'])