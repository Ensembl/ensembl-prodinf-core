# See the NOTICE file distributed with this work for additional information
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
import urllib3
from elasticsearch import Elasticsearch


class ElasticsearchException(Exception):
    """Kept for backward compatibility: elasticsearch-py 8.x removed its own
    shared base exception (split into ApiError/TransportError hierarchies)."""


class ElasticsearchConnectionManager:
    def __init__(self, host: str, port: int, user: str = "", password: str = "", with_ssl: bool = False):
        self.host = host
        self.port = port
        self.user = user
        self.password = password
        self.ssl = with_ssl
        self.client = None

    def __enter__(self):
        urllib3.disable_warnings(category=urllib3.exceptions.InsecureRequestWarning)
        scheme = "https" if self.ssl else "http"
        # Strip any scheme the caller may have already included in `host`,
        # since the client requires a single well-formed scheme://host:port URL.
        bare_host = self.host.split("://", 1)[-1]
        self.client = Elasticsearch(f"{scheme}://{bare_host}:{self.port}",
                                    basic_auth=(self.user, self.password) if self.user else None,
                                    verify_certs=False,
                                    ssl_show_warn=False)
        if not self.client.ping():
            raise RuntimeError(
                f"Cannot connect to Elasticsearch server. User: {self.user}, Host: {self.host}:{self.port}")
        return self

    def __exit__(self, exc_type, exc_value, exc_traceback):
        self.client.close()
