#!/usr/bin/env python3

import contextlib
import glob
import json
import os

from testgres import get_new_node

sources = [
    ('football', './football.json'),
]

conf = '''
shared_preload_libraries='jsonbd'
'''


@contextlib.contextmanager
def cwd(path):
    print("cwd: ", path)
    curdir = os.getcwd()
    os.chdir(path)

    try:
        yield
    finally:
        print("cwd:", curdir)
        os.chdir(curdir)


with get_new_node('node1') as node:
    node.init()
    node.append_conf('postgresql.conf', conf)
    node.start()

    node.safe_psql('create extension jsonbd')

    for name, root_dir in sources:
        table_name = name
        table_name_c = '%s_c' % name

        node.safe_psql('create table %s(a jsonb)' % table_name)
        node.safe_psql('create table %s(a jsonb compression jsonbd)' % table_name_c)
        node.safe_psql('alter table %s alter column a set storage external' % table_name)

        with node.connect() as con:
            with cwd(os.path.abspath(root_dir)):
                for filename in glob.iglob('**/*.json', recursive=True):
                    if filename == 'package.json':
                        continue

                    with open(filename, 'r') as f:
                        data = json.load(f)

                    if isinstance(data, dict):
                        if 'rounds' in data:
                            for obj in data['rounds']:
                                sql = 'insert into {} values (%s)'
                                for i in range(10):
                                    con.execute(sql.format(table_name), (json.dumps(obj), ))
                                    con.execute(sql.format(table_name_c), (json.dumps(obj), ))

        print(node.safe_psql("select pg_size_pretty(pg_total_relation_size('%s'))" % table_name))
        print(node.safe_psql("select pg_size_pretty(pg_total_relation_size('%s'))" % table_name_c))
