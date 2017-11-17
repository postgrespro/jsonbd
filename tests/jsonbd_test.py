#!/usr/bin/env python3

import unittest
import random
import json

from testgres import get_new_node


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
    def test_correctness(self):
        with get_new_node('node1') as node:
            node.init()
            node.append_conf("postgresql.conf", "shared_preload_libraries='jsonbd'\n")
            node.start()

            node.psql('postgres', 'create extension jsonbd')
            node.psql('postgres', 'create compression method cm1 handler jsonbd_compression_handler')
            node.psql('postgres', 'create table t1(pk serial, a jsonb compressed cm1);')

            data = []
            for i in range(100):
                d = generate_dict()
                data.append(d)
                node.psql('postgres', "begin; insert into t1 (a) values ('%s'); commit;" % json.dumps(d))

            with node.connect('postgres') as con:
                res = con.execute('select pk, a from t1 order by pk')
                for pk, val in res:
                    self.assertEqual(val, data[pk - 1])


if __name__ == "__main__":
    unittest.main()
