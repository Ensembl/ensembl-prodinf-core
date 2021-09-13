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
import pathlib

from ensembl.production.core.config import load_config_yaml

logging.basicConfig()

here = pathlib.Path(__file__).parent.resolve()

class YamlLoaderTest(unittest.TestCase):
    def test_parse_simple_str(self):
        home_dir = os.getenv("HOME")
        os.environ['EXPECTEDVAR'] = 'environmentVar'
        logging.info("Expected Home is %s" % home_dir)
        config = load_config_yaml(here/'sample_config.yaml')
        self.assertIn(home_dir, config['base_dir'], "Home dir has been replaced")
        self.assertIn('environmentVar', config['server_names_file'], "Env var has been replaced in line")
        self.assertIn(home_dir, config['server_names_file'], "Home dir has been replaced in other line")
