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

import unittest
from ensembl.production.core.db_introspects import _apply_filters


class DBIntrospectsTest(unittest.TestCase):
    def test_apply_filters_no_filters(self):
        names_list = [
            'species1_core_105_1',
            'species2_core_105_1',
            'species3_core_104_2',
            'species4_core_104_2',
        ]
        result = _apply_filters(names_list, incl_filters=None, skip_filters=None)
        self.assertSetEqual(set(names_list), result)

    def test_apply_filters_incl_filters(self):
        names_list = [
            'species1_core_105_1',
            'species2_core_105_1',
            'species3_core_104_2',
            'species4_core_104_2',
        ]
        incl_filters = ['.*core_105.*']
        expected = [
            'species1_core_105_1',
            'species2_core_105_1',
        ]
        result = _apply_filters(names_list, incl_filters=incl_filters, skip_filters=None)
        self.assertSetEqual(set(expected), result)

    def test_apply_filters_incl_filters_no_wildcards(self):
        names_list = [
            'species1_core_105_1',
            'species2_core_105_1',
            'species3_core_104_2',
            'species4_core_104_2',
        ]
        incl_filters = ['core_105']
        expected = []
        result = _apply_filters(names_list, incl_filters=incl_filters, skip_filters=None)
        self.assertSetEqual(set(expected), result)

    def test_apply_filters_incl_filters_and_matches(self):
        names_list = [
            'species1_core_105_1',
            'species2_core_105_1',
            'species3_core_104_2',
            'species4_core_104_2',
        ]
        incl_filters = ['.*core_105.*', 'species4_core_104_2']
        expected = [
            'species1_core_105_1',
            'species2_core_105_1',
            'species4_core_104_2',
        ]
        result = _apply_filters(names_list, incl_filters=incl_filters, skip_filters=None)
        self.assertSetEqual(set(expected), result)

    def test_apply_filters_incl_filters_and_matches_skip_filters(self):
        names_list = [
            'species1_core_105_1',
            'species2_core_105_1',
            'species3_core_104_2',
            'species4_core_104_2',
        ]
        incl_filters = ['.*core_105.*', 'species4_core_104_2']
        skip_filters = ['species2.*']
        expected = [
            'species1_core_105_1',
            'species4_core_104_2',
        ]
        result = _apply_filters(names_list, incl_filters=incl_filters, skip_filters=skip_filters)
        self.assertSetEqual(set(expected), result)

    def test_apply_filters_incl_filters_and_matches_skip_filters_no_wildcards(self):
        names_list = [
            'species1_core_105_1',
            'species2_core_105_1',
            'species3_core_104_2',
            'species4_core_104_2',
        ]
        incl_filters = ['.*core_105.*', 'species4_core_104_2']
        skip_filters = ['species2']
        expected = [
            'species1_core_105_1',
            'species2_core_105_1',
            'species4_core_104_2',
        ]
        result = _apply_filters(names_list, incl_filters=incl_filters, skip_filters=skip_filters)
        self.assertSetEqual(set(expected), result)

    def test_apply_filters_incl_filters_and_matches_skip_filters_and_matches(self):
        names_list = [
            'species1_core_105_1',
            'species2_core_105_1',
            'species3_core_104_2',
            'species4_core_104_2',
        ]
        incl_filters = ['.*core_105.*', 'species4_core_104_2']
        skip_filters = ['species2.*', 'species1_core_105_1']
        expected = [
            'species4_core_104_2',
        ]
        result = _apply_filters(names_list, incl_filters=incl_filters, skip_filters=skip_filters)
        self.assertSetEqual(set(expected), result)

    def test_apply_filters_skip_filters(self):
        names_list = [
            'species1_core_105_1',
            'species2_core_105_1',
            'species3_core_104_2',
            'species4_core_104_2',
        ]
        skip_filters = ['.*core_104.*']
        expected = [
            'species1_core_105_1',
            'species2_core_105_1',
        ]
        result = _apply_filters(names_list, incl_filters=None, skip_filters=skip_filters)
        self.assertSetEqual(set(expected), result)

    def test_apply_filters_skip_filters_and_matches(self):
        names_list = [
            'species1_core_105_1',
            'species2_core_105_1',
            'species3_core_104_2',
            'species4_core_104_2',
        ]
        skip_filters = ['.*core_104.*', 'species1_core_105_1']
        expected = [
            'species2_core_105_1',
        ]
        result = _apply_filters(names_list, incl_filters=None, skip_filters=skip_filters)
        self.assertSetEqual(set(expected), result)

