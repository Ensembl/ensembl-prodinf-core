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
from functools import lru_cache
import sqlalchemy as sa
import re


@lru_cache(maxsize=None)
def get_engine(hostname, user='mysql', port='3306', password=''):
    uri = 'mysql://{}:{}@{}:{}'.format(user,
                                       password,
                                       hostname,
                                       port)
    return sa.create_engine(uri, pool_recycle=3600)


def get_database_set(hostname, port, name_filter='', name_matches=None, excluded_schemas=None):
    excluded = excluded_schemas or set()
    matches = name_matches or []
    try:
        db_engine = get_engine(hostname, port)
    except RuntimeError as e:
        raise ValueError('Invalid hostname: {} or port: {}'.format(hostname, port)) from e
    database_list = sa.inspect(db_engine).get_schema_names()
    if matches:
        database_set = set(database_list)
        names_set = set(matches)
        return database_set.difference(excluded).intersection(names_set)
    else:
        try:
            filter_db_re = re.compile(name_filter)
        except re.error as e:
            raise ValueError('Invalid name_filter: {}'.format(name_filter)) from e
        return set(filter(filter_db_re.search, database_list)).difference(excluded)


def get_table_set(hostname, port, database, name_filter='', name_matches=[], excluded_tables=None):
    excluded = excluded_tables or set()
    try:
        filter_table_re = re.compile(name_filter)
    except re.error as e:
        raise ValueError('Invalid name_filter: {}'.format(name_filter)) from e
    try:
        db_engine = get_engine(hostname, port)
    except RuntimeError as e:
        raise ValueError('Invalid hostname: {} or port: {}'.format(hostname, port)) from e
    try:
        table_list = sa.inspect(db_engine).get_table_names(schema=database)
    except sa.exc.OperationalError as e:
        raise ValueError('Invalid database: {}'.format(database)) from e
    if name_matches:
        table_set = set(table_list)
        table_names_set = set(name_matches)
        return table_set.difference(excluded).intersection(table_names_set)
    return set(filter(filter_table_re.search, table_list)).difference(excluded)
