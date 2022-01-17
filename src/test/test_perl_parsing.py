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
import unittest

from ensembl.production.core.db_utils import validate_mysql_url


class TestPerlParsing(unittest.TestCase):

    def test_extra_hidden_character(self):
        """
        Use case tested for parsing output from metadata updated, ensure the hidden chars \t\r\n are actually
        removed from initial string.
        :return: None
        """
        from ensembl.production.core.perl_utils import perl_string_to_python
        test_string = '{"comment" => "Handover 105 core of new Metazoan species database agrilus	planipennis", ' \
                      '"database_uri" => "mysql://ensuser:enspass\@localhost:3306/agrilus_planipennis_' \
                      'gca000699045v2_core_105_1", "email" => "test\@ebi.ac.uk", "metadata_uri" => ' \
                      '"mysql://ensuser:enspass\@localhost:3306/ensembl_metadata_qrp", ' \
                      '"source" => "Handover", "timestamp" => "Thu Jan 13 17:45:37 2022"}'

        data_python = perl_string_to_python(test_string)
        self.assertEqual(len(data_python.keys()), 6)
        self.assertIn('comment', data_python.keys())
        self.assertNotIn("\t", data_python['comment'])
        db_uri = validate_mysql_url(data_python['database_uri'])
        self.assertEqual(db_uri, 'mysql://ensuser:enspass@localhost:3306/agrilus_planipennis_gca000699045v2_core_105_1')

    def test_compat_import(self):
        from ensembl.production.core.utils import perl_string_to_python
        perl_string = '{"testKey" => "testValue\@testhost"}'
        with self.assertWarns(DeprecationWarning):
            data_python = perl_string_to_python(perl_string)
            self.assertEqual(len(data_python.keys()), 1)
            self.assertEqual(data_python['testKey'], 'testValue@testhost')
