# .. See the NOTICE file distributed with this work for additional information
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

import json
import logging
import yaml

logger = logging.getLogger(__name__)


def load_config_yaml(file_path, strict=False):
    if file_path:
        with open(file_path, 'r') as f:
            config = yaml.safe_load(f)
        return config if config else {}
    else:
        if strict:
            raise ValueError('Invalid config file path: %s' % file_path)
        else:
            logger.warning('Using default configuration. Config file path was: %s', file_path)
            return {}


def parse_debug_var(var):
    return not ((str(var).lower() in ('f', 'false', 'no', 'none')) or (not var))


def load_config_json(file_path):
    with open(file_path, 'r') as f:
        config = json.load(f)
    return config if config else {}
