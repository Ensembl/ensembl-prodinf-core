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

from ensembl.production.core.perl_utils import dict_to_perl_string, perl_string_to_python

logging.basicConfig()


class UtilsTest(unittest.TestCase):

    def test_parse_simple_str(self):
        o = {'a': 'b'}
        s = dict_to_perl_string(o)
        self.assertEqual(s, """{"a" => "b"}""")
        o2 = perl_string_to_python(s)
        t = type(o2).__name__
        self.assertEqual(t, "dict")
        self.assertEqual(o2['a'], 'b')

    def test_parse_simple_str_pair(self):
        o = {'a': 'b', 'c': 'd'}
        s = dict_to_perl_string(o)
        self.assertEqual(s, """{"a" => "b", "c" => "d"}""")
        o2 = perl_string_to_python(s)
        t = type(o2).__name__
        self.assertEqual(t, "dict")
        self.assertEqual(o2['a'], 'b')
        self.assertEqual(o2['c'], 'd')

    def test_parse_simple_int(self):
        o = {'a': 99}
        s = dict_to_perl_string(o)
        self.assertEqual(s, """{"a" => 99}""")
        o2 = perl_string_to_python(s)
        t = type(o2).__name__
        self.assertEqual(t, "dict")
        self.assertEqual(o2['a'], 99)

    def test_parse_simple_list(self):
        o = {'a': ["a", "b", "c"]}
        s = dict_to_perl_string(o)
        self.assertEqual(s, """{"a" => ["a", "b", "c"]}""")
        o2 = perl_string_to_python(s)
        t = type(o2).__name__
        self.assertEqual(t, "dict")
        self.assertEqual(type(o2['a']).__name__, "list")

    def test_parse_complex(self):
        o = {'a': ["a", "b", "c"], 'b': 'd', 'c': 99}
        s = dict_to_perl_string(o)
        self.assertEqual(s, """{"a" => ["a", "b", "c"], "b" => "d", "c" => 99}""")
        o2 = perl_string_to_python(s)
        t = type(o2).__name__
        self.assertEqual(t, "dict")
        self.assertEqual(type(o2['a']).__name__, "list")
        self.assertEqual(o2['b'], 'd')
        self.assertEqual(o2['c'], 99)

    def test_parse_string_quotes(self):
        o = {'a': "\"b\""}
        s = dict_to_perl_string(o)
        self.assertEqual(s, """{"a" => "\\"b\\""}""")
        o2 = perl_string_to_python(s)
        t = type(o2).__name__
        self.assertEqual(t, "dict")
        self.assertEqual(o2['a'], '"b"')

    def test_parse_string_dollar(self):
        o = {'a': "$b"}
        s = dict_to_perl_string(o)
        self.assertEqual(s, """{"a" => "\\$b"}""")
        o2 = perl_string_to_python(s)
        t = type(o2).__name__
        self.assertEqual(t, "dict")
        self.assertEqual(o2['a'], '$b')

    def test_parse_string_at(self):
        o = {'a': "@b"}
        s = dict_to_perl_string(o)
        self.assertEqual(s, """{"a" => "\\@b"}""")
        o2 = perl_string_to_python(s)
        t = type(o2).__name__
        self.assertEqual(t, "dict")
        self.assertEqual(o2['a'], '@b')

    def test_parse_none(self):
        o = {'a': None}
        s = dict_to_perl_string(o)
        self.assertEqual(s, """{}""")
