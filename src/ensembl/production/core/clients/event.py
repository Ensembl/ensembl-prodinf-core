#   See the NOTICE file distributed with this work for additional information
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

import requests

from ensembl.production.core.rest import RestClient


class EventClient(RestClient):
    """
    Simple client for submitting an event to the event service and checking on progress
    This uses the base RestClient, but all endpoint URIs for checking on submited events
    have process as a path element, so this client combines the job_id and process together
    """

    def submit_job(self, event):
        """Submit an event for processing"""
        logging.info("Submitting job")
        return RestClient.submit_job(self, event)

    def list_jobs(self, process):
        """List all jobs for a given process"""
        logging.info("Listing")
        r = requests.get(self.jobs.format(self.uri) + '/' + process)
        r.raise_for_status()
        return r.json()

    def delete_job(self, process, job_id, kill=False):
        return super(EventClient, self).delete_job(process + '/' + str(job_id), kill)

    def retrieve_job_failure(self, process, job_id):
        return super(EventClient, self).retrieve_job_failure(process + '/' + str(job_id))

    def retrieve_job_email(self, process, job_id):
        return super(EventClient, self).retrieve_job_email(process + '/' + str(job_id))

    def retrieve_job(self, process, job_id):
        return super(EventClient, self).retrieve_job(process + '/' + str(job_id))

    def collate_jobs(self, output_file, pattern='.*'):
        raise AttributeError("Job collation not supported")

    def processes(self):
        """Retrieve a list of processes that the service can schedule"""
        r = requests.get(self.uri + 'processes')
        r.raise_for_status()
        return r.json()

    def events(self):
        """Retrieve a list of events that the service can handle"""
        r = requests.get(self.uri + 'events')
        r.raise_for_status()
        return r.json()
