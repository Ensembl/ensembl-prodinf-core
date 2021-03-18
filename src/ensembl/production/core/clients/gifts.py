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
import json
import logging
import re

from ensembl.production.core.rest import RestClient


class GIFTsClient(RestClient):
    """Client for interacting with the GIFTs services"""

    def submit_job(self, email, environment, tag, ensembl_release):
        """
        Start a GIFTs pipeline.
        Arguments:
          ensembl_release - mandatory Ensembl release number
          environment - mandatory execution environment (dev or staging)
          email - mandatory address for an email on job completion
          tag - optional text for annotating a submission
        """

        payload = {
            'ensembl_release': ensembl_release,
            'environment': environment,
            'email': email,
            'tag': tag
        }

        return RestClient.submit_job(self, payload)

    def list_jobs(self, output_file, pattern):
        """
        Find jobs and print results
        Arguments:
          output_file - optional file to write report
          pattern - optional pattern to filter jobs by
        """
        jobs = super(GIFTsClient, self).list_jobs()
        if pattern is None:
            pattern = '.*'
        tag_pattern = re.compile(pattern)
        output = []
        for job in jobs:
            if 'tag' in job['input']:
                tag = job['input']['tag']
            else:
                tag = ''
            if tag_pattern.search(tag):
                output.append(job)

        if output_file is None:
            print(json.dumps(output, indent=2))
        else:
            output_file.write(json.dumps(output))

    def print_job(self, job, print_results=False, print_input=False):
        """
        Render a job to logging
        Arguments:
          job :  job to print
          print_results : set to True to print detailed results
          print_input : set to True to print input for job
        """
        logging.info("Job %s - %s" % (job['id'], job['status']))
        if print_input == True:
            self.print_inputs(job['input'])
        if job['status'] == 'complete':
            if print_results == True:
                logging.info("Submission status: " + str(job['status']))
        elif job['status'] == 'incomplete':
            if print_results == True:
                logging.info("Submission status: " + str(job['status']))
        elif job['status'] == 'failed':
            logging.info("Submission status: " + str(job['status']))
            # failures = self.retrieve_job_failure(job['id'])
            # logging.info("Error: " + str(failures))
        else:
            raise ValueError("Unknown status {}".format(job['status']))

    def print_inputs(self, i):
        """Utility to render a job input dict to logging"""
        if 'ensembl_release' in i:
            logging.info("Ensembl Release: " + i['ensembl_release'])
        if 'environment' in i:
            logging.info("Environment: " + i['environment'])
        if 'email' in i:
            logging.info("Email: " + i['email'])
        if 'tag' in i:
            logging.info("Tag: " + i['tag'])
