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


def generate_keys(count):
    population = 'qwertyuiopsadfghjklzxcvbnm1234567890'
    result = []
    for i in range(count):
        keylen = random.randint(1, len(population))
        key = ''.join(random.sample(population, keylen))
        result.append(key)

    return result


def generate_dict(keys):
    res = {}

    for i in range(100):
        res[keys[random.randint(0, len(keys) - 1)]] = random.randint(1, 10000)

    return res


KEYS = ['pu4rj8cin2vthkzx3gm79q1ea6wlb5sdfy0', 'en4rl5h01mpwocydx9', 'hui0gen37qv1zf5kjw8lp2d6ramst4bx', 'macvjs35y1xneodi', 'r', 'gev6qfyb57dakwhx803umnczi4pj2lrst', 'pzkd5n4ufcaj', 'wubzi', 'h', 'ca6', 'krypftxe8ovbu3i2dh', '5y70', 'of2zcp8rgq0kmntu9yv314eb6ws7jahl', '2yu1iv645cwhepkmasnzfrl7gjq9x8td', 'ysoikbdwj8l7hrv4ag', 'q4s7xugt9bnzkw125vl60rcojayh38dimepf', 'ijbtvadn9x', '0aonrspwhbdvzg2lq8cuef', 'hf2ybkcvl8eaj9m503o4dtnrguxq6w', 'ayr0', 'r612my98ehwsui7bo30vk4c5djlxtazgn', 'ancd7fe8qh65s', 'ghptl062z5mwr7fqae', 'hgf1j7myqo0vrpbd93se4ztu6i28lnxwack5', 'ibx6cje7rlof0ukyh54apvs', 'w1xhvfu', 'nb8zf601tjmi29q5pyexw7gdlk', 'f6v4xjn9ylr2m', 'uoai017bfth4gxwjsep3y28kz', 'pg', 't', '5w61bms8cjoayr93ixtehg4p7uq0n2vf', 'u53k0nfswaoyjx19vdp8it', 'u4rlpft3qh6gjkacs1e28b5wimvx0yz', '0249or1fze', 'd4uey10zbhtf9jla5gqs', '8k0', '21sjt8ap7u5vxhkyeo0zf4ic9gqrw63d', 'pvih7546ea3cbgxuk', 'uarnfycxib74926jt1lgqhm0kw5esp83v', 'zrwmkgs06yv', 'v5igznkpjle8632cuyfdxq1a9mthwsbo704', '5y3ktwe6hxcl9rfdsn4z7uqjg', 'ly3f6centogzb82us9wp', 'o0gqlpn31sw845i9eukv', '1ho5uk97azcd', '8723gtsz6a9fcbo', 'nfxi8sl20r9kbdz6t35meqoapygucvhj4w17', 'dgutb4fj2ceqsyz750o', 'c65', 'do', 'sohb1gfea6cnxyd92qv78p4w0tkuzm5jlr3', 'iedsfb6mgl85zh32krtx1v94', 'igz27dvfwx45qh', '8qxwg3fcm6dba7rk50ntjohslze', '9mj5a1f8rxpb6s30kdlnw7guitcve', '2zlae93nxf480ykuojc1hw7qirmt6', '4abs05mnug7tpvjrlx3q18fc', '57w0g1tsbrun2hk4', 'z2yh7ojkx9p0', 'w', 'bgp3sefz5vrkot87uqh', 'a3h', '4zg9i3bmeqd52vpft0laruj7hksn', 'tkenuwzqy35hv2dbof4clm', 'fydqn290lwxrpus8ka', 'zmg7lx0df1qewt', 'k2cegpz0dq6r4uiwovhmj', 'tci91qj', 's1ug4t85wca0hnmlpfo', 'bca42i1pu7h3dolvkme9yn8fw05qrgx6', 'kw72sdb', 'jc5sa9zqhmb21v36tdpk0f48', 'w8fvk4751xqdyjp6eolcgsamn9z20rb3tu', '64b8mz9017nq3xyec', '5gju7r9ae30xzh8inp6b1', '02zcf94mojtyxk75bs8a', 'n18com3dzihj6upxkb0wgat2sverlqf59y74', 'qbofcwz15h9e24j6mul3rd', '0topwjfmd3z849kenu2vsxqac1yih5gl7rb6', 'i0319k', '40g83qtbdih69vr1ol', '4lpxsfj1q2eztguok3wa6cr9db', '2vl', '2rq1b90zojetxas6v47lkyn38dwgpc5hi', 'f9dcobjw6xy1', 'dhue8c3t0gi4vnz', '7bkxn9po46', 'gj0q4it6d2s7mkzf5xlo1ha9y8pucr3', 'rucdks4w01', 'uo6txm2gwf4k38ijav9150cdhbrlqzpnesy', '0d4gec8txisa2r5f3mwnjhl', 'p8cqeoi071bt4hyw6fzv', '10g3b', 'zhrolvm5c', '2iroh6pz3l5b8vkfens1caym9qg0x', 'sm5wftozqbkd4py132u', 'cvyfqruz', 'bcmq2nag6hu3j0otyk8iz9evp4fr7s5dw1', 'pnqc7xy06bfiv14zg'] 

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
                for i in range(10000):
                    d = generate_dict(KEYS)
                    data.append(d)
                    con.execute("insert into t1 (a) values ('%s');" % json.dumps(d))

                res = con.execute('select pk, a from t1 order by pk')
                for pk, val in res:
                    self.assertEqual(val, data[pk - 1])

            data = node.psql('postgres', "select pg_size_pretty(pg_total_relation_size('t1'))")
            print("Relation size: ", data[1].decode('utf-8'))


if __name__ == "__main__":
    unittest.main()
