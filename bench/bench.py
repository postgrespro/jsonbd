#!/usr/bin/env python3

import asyncio
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


async def insert_data(con, files, table_name, table_name_c):
    while True:
        try:
            filename = files.pop()
            print(filename)
        except IndexError:
            break

        with open(filename, 'r') as f:
            data = json.load(f)

        if isinstance(data, dict):
            if 'rounds' in data:
                for obj in data['rounds']:
                    sql = 'insert into {} values ($1)'
                    for i in range(100):
                        await con.execute(sql.format(table_name), json.dumps(obj))
                        await con.execute(sql.format(table_name_c), json.dumps(obj))


def main(loop):
    with get_new_node('node1') as node:
        node.init()
        node.append_conf('postgresql.conf', conf)
        node.start()

        node.safe_psql('postgres', 'create extension jsonbd')

        connections = [node.connect() for i in range(3)]

        for name, root_dir in sources:
            table_name = name
            table_name_c = '%s_c' % name

            node.safe_psql('postgres', 'create table %s(a jsonb)' % table_name)
            node.safe_psql('postgres', 'create table %s(a jsonb compression jsonbd)' % table_name_c)
            node.safe_psql('postgres', 'alter table %s alter column a set storage external' % table_name)

            files = []
            with cwd(os.path.abspath(root_dir)):
                for filename in glob.iglob('**/*.json', recursive=True):
                    if filename == 'package.json':
                        continue

                    files.append(filename)

                coroutines = [insert_data(con, files, table_name, table_name_c)
                        for con in connections]
                loop.run_until_complete(asyncio.gather(*coroutines))

                print(node.safe_psql('postgres', "select pg_size_pretty(pg_total_relation_size('%s'))" % table_name))
                print(node.safe_psql('postgres', "select pg_size_pretty(pg_total_relation_size('%s'))" % table_name_c))


if __name__ == '__main__':
    loop = asyncio.get_event_loop()
    main(loop)
    loop.close()
