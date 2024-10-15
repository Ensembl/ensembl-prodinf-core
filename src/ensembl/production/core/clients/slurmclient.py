import requests
import json
import os

class SlurmRestClient:
    def __init__(self, base_url, user_name, version="v0.0.40", token=None, secret_file=None):
        """
        Initialize the SLURM REST client with user-specific headers. Token can be provided,
        fetched from an environment variable, or read from a secret file.

        :param base_url: Base URL for SLURM REST API.
        :param user_name: SLURM user name for authentication.
        :param version: SLURM API version (default is 'v0.0.40').
        :param token: Optional SLURM JWT token for authentication.
        :param secret_file: Path to a secret file containing the SLURM JWT token (optional).
        """
        self.base_url = base_url
        self.version = version
        self.headers = {
            'Content-Type': 'application/json',
            'X-SLURM-USER-NAME': user_name
        }
        
        # Check if token is provided, in environment, or in secret file
        self.token = token or os.getenv('SLURM_JWT') or self._read_token_from_file(secret_file)
        
        # If we have a token, include it in the headers
        if self.token:
            self.headers['X-SLURM-USER-TOKEN'] = self.token

    def _read_token_from_file(self, secret_file):
        """
        Reads the SLURM JWT token from a secret file if provided.

        :param secret_file: Path to the secret file.
        :return: Token string or None if file is not found or not provided.
        """
        if secret_file and os.path.exists(secret_file):
            with open(secret_file, 'r') as f:
                return f.read()
        return None

    def check_cluster_health(self):
        """
        Check SLURM cluster health by calling the ping endpoint.

        :return: Dictionary containing cluster health status.
        """
        url = f"{self.base_url}/slurm/{self.version}/ping"
        response = requests.get(url, headers=self.headers)
        if response.status_code == 200:
            return response.json()
        else:
            response.raise_for_status()

    def submit_job(self, script, job_name, cwd, env_vars, time_limit, memory_per_node):
        """
        Submit a job to SLURM using a JSON payload.

        :param script: The SLURM batch script content.
        :param job_name: Job name.
        :param cwd: Current working directory where the job will be executed.
        :param env_vars: Environment variables list (e.g., ["VAR1=value1", "VAR2=value2"]).
        :param time_limit: Time limit for the job in minutes.
        :param memory_per_node: Memory limit per node in MB.

        :return: Dictionary with the submission response.
        """
        url = f"{self.base_url}/slurm/{self.version}/job/submit"
        
        # Construct the JSON payload with the required structure
        job_data = {
            "script": script,
            "job": {
                "environment": env_vars,
                "current_working_directory": cwd,
                "name": job_name,
                "time_limit": time_limit,
                "memory_per_node": {
                    "set": True,
                    "number": memory_per_node
                }
            }
        }
        
        response = requests.post(url, headers=self.headers, data=json.dumps(job_data))
        if response.status_code == 200:
            return response.json()  # Return the JSON response with job ID and details
        else:
            response.raise_for_status()

    def retrieve_all_jobs(self):
        """
        Retrieve all running jobs.

        :return: Dictionary containing details of all running jobs.
        """
        url = f"{self.base_url}/slurm/{self.version}/jobs"
        response = requests.get(url, headers=self.headers)
        if response.status_code == 200:
            return response.json()
        else:
            response.raise_for_status()

    def retrieve_job_states(self):
        """
        Retrieve status for all current jobs.

        :return: Dictionary containing the status of all current jobs.
        """
        url = f"{self.base_url}/slurm/{self.version}/jobs/state"
        response = requests.get(url, headers=self.headers)
        if response.status_code == 200:
            return response.json()
        else:
            response.raise_for_status()

    def get_job_information(self, job_id):
        """
        Get detailed information for a specific job by job ID.

        :param job_id: Job ID to query.
        :return: Dictionary containing detailed job information.
        """
        url = f"{self.base_url}/slurm/{self.version}/job/{job_id}"
        response = requests.get(url, headers=self.headers)
        if response.status_code == 200:
            return response.json()
        else:
            response.raise_for_status()

    def retrieve_node_status(self):
        """
        Retrieve status information for all nodes.

        :return: Dictionary containing the node status information.
        """
        url = f"{self.base_url}/slurm/{self.version}/nodes"
        response = requests.get(url, headers=self.headers)
        if response.status_code == 200:
            return response.json()
        else:
            response.raise_for_status()

    def cancel_job(self, job_id):
        """
        Cancel a specific job by job ID.

        :param job_id: Job ID to cancel.
        :return: Dictionary containing the cancellation response.
        """
        url = f"{self.base_url}/slurm/{self.version}/job/{job_id}"
        response = requests.delete(url, headers=self.headers)
        if response.status_code == 200:
            return response.json()
        else:
            response.raise_for_status()

    def get_job_details_from_db(self, job_id):
        """
        Get detailed job information from SLURM DB by job ID.

        :param job_id: Job ID to query in SLURM DB.
        :return: Dictionary containing job details from SLURM DB.
        """
        url = f"{self.base_url}/slurmdb/{self.version}/job/{job_id}"
        response = requests.get(url, headers=self.headers)
        if response.status_code == 200:
            return response.json()
        else:
            response.raise_for_status()