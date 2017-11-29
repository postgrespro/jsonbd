#!/usr/bin/env python3

import unittest
import random
import json
import os.path
import subprocess

from testgres import get_new_node

# set setup base logging config, it can be turned on by `use_logging`
# parameter on node setup

import logging
import logging.config

logfile = os.path.join(os.path.dirname(os.path.realpath(__file__)), 'tests.log')
LOG_CONFIG = {
    'version': 1,
    'handlers': {
        'console': {
            'class': 'logging.StreamHandler',
            'formatter': 'base_format',
            'level': logging.DEBUG,
        },
        'file': {
            'class': 'logging.FileHandler',
            'filename': logfile,
            'formatter': 'base_format',
            'level': logging.DEBUG,
        },
    },
    'formatters': {
        'base_format': {
            'format': '%(node)-5s: %(message)s',
        },
    },
    'root': {
        'handlers': ('file', ),
        'level': 'DEBUG',
    },
}

logging.config.dictConfig(LOG_CONFIG)

insert_cmd = '''
	INSERT INTO comp.t
		SELECT jsonb_object(array_agg(array[repeat(letter, count), count::text]))
	FROM (
		SELECT chr(i) AS letter, b AS count
		FROM generate_series(ascii('a'), ascii('z')) i
		FULL OUTER JOIN
		(SELECT b FROM generate_series(10, 20) b) t2
		ON 1=1
	) t3;
'''

def generate_dict():
    population = 'qwertyuiopsadfghjklzxcvbnm1234567890'
    res = {}

    for i in range(300):
        keylen = random.randint(1, len(population))
        key = ''.join(random.sample(population, keylen))
        res[key] = keylen

    return res


class Tests(unittest.TestCase):
    def set_trace(self, con, command="pg_debug"):
        pid = con.execute("select pg_backend_pid()")[0][0]
        p = subprocess.Popen([command], stdin=subprocess.PIPE)
        p.communicate(str(pid).encode())

    def test_correctness(self):
        with get_new_node('node1') as node:
            node.init()
            node.append_conf("postgresql.conf", "shared_preload_libraries='jsonbd'\n")
            node.start()

            node.psql('postgres', 'create extension jsonbd')
            node.psql('postgres', 'create table t1(pk serial, a jsonb compression jsonbd);')

            data = []
            with node.connect('postgres') as con:
                for i in range(1000):
                    d = generate_dict()
                    data.append(d)
                    con.execute("insert into t1 (a) values ('%s');" % json.dumps(d))

                res = con.execute('select pk, a from t1 order by pk')
                for pk, val in res:
                    self.assertEqual(val, data[pk - 1])


if __name__ == "__main__":
    unittest.main()
