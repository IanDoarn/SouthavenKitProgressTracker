import cx_Oracle
import psycopg2
import copy
import json
import re
from settings import \
    ORACLE_CONNECTION_STRING, \
    PORSTGRES_CONNECTION_STRING, \
    NEW_KIT_REGEX_PATTERN, \
    KITS_TO_TRACK_TABLE, \
    KIT_PROGRESS_TABLE


ORACLE_CONNECTION = cx_Oracle.connect(ORACLE_CONNECTION_STRING)
POSTGRES_CONNECTION = psycopg2.connect(PORSTGRES_CONNECTION_STRING)

ORACLE_CURSOR = ORACLE_CONNECTION.cursor()
POSTGRES_CURSOR = POSTGRES_CONNECTION.cursor()

TRUNCATE_TABLE = lambda x: r"TRUNCATE TABLE {} CONTINUE IDENTITY RESTRICT;".format(x)

def load_query_from_file(file, *args):
    with open(file, 'r')as qf:
        q = qf.read()
        if len(args) > 0:
            return q.format(*args)
        return q


def f_results(cur, file, *args):
    fd = []
    data = cur.execute(load_query_from_file(file, *args))
    headers = tuple(i[0] for i in cur.description)
    results = [row for row in data]
    for row in results:
        fd.append(
            {i[0]: i[1] for i in list(zip(headers, row))}
        )
    return headers, results, fd


def insert_into_postgres_table(cursor, table_name, table_headers, data, truncate=False):
    print('Inserting {} rows into table [{}]'.format(str(len(data)), table_name))

    if truncate:
        print('Truncating table {}'.format(table_name))
        cursor.execute(TRUNCATE_TABLE(table_name))

    for row in data:
        stmt = r"INSERT INTO {}{} VALUES {}".format(
            table_name, str(tuple(table_headers)).replace("'", ''), str(tuple(row))
        )

        cursor.execute('SAVEPOINT sp1')

        try:
            cursor.execute(stmt)
        except Exception as error:
            print(error)
            print(cursor.statusmessage)
            cursor.execute("ROLLBACK TO SAVEPOINT sp1")
        except psycopg2.IntegrityError:
            cursor.execute("ROLLBACK TO SAVEPOINT sp1")
        else:
            print(stmt)
            cursor.execute('RELEASE SAVEPOINT sp1')


def update_new_kit_data_file(new_kit_file, json_file):
    FILE = new_kit_file
    J_FILE = json_file

    with open(FILE, 'r')as f:
        n_data = f.readlines()

    with open(J_FILE, 'r')as jf:
        j_data = json.load(jf)

    n_data = [x.replace('\n', '') for x in n_data]
    n_kits = {}
    j_kits = copy.deepcopy(j_data['data'])

    for l in n_data:
        m = re.search(NEW_KIT_REGEX_PATTERN, l)
        n_kits[m.group(1)] = m.group(4).split(',')

    for k, v in n_kits.items():
        if k in j_kits.keys():
            j_kits[k].extend(v)
        else:
            j_kits[k] = v

    for k, v in j_kits.items():
        j_kits[k] = sorted(list(set([int(x) for x in v])))

    data = copy.deepcopy(j_kits)
    kits = sorted(list(data.keys()))
    serials = sorted([int(x) for x in set(sum(data.values(), []))])

    with open(J_FILE, 'w')as jf:
        json.dump(
            {'data': data, 'kits': kits, 'serials': serials},
            jf, indent=4, ensure_ascii=True
        )

    return j_kits


if __name__ == '__main__':

    POSTGRES_CURSOR.execute(load_query_from_file('POSTGRES_CREATE_KIT_TRACKER_TABLE.sql'))
    POSTGRES_CURSOR.execute(load_query_from_file('POSTGRES_CREATE_NEW_KITS_TABLE.sql'))

    new_kit_data = update_new_kit_data_file('3DAY_NEW_KIT_STOCK.TXT', '3DAY_NEW_KIT_STOCK.json')

    for kit, serials in new_kit_data.items():
        nki_headers, nki_results, _ = f_results(
            ORACLE_CURSOR,
            'ORACLE_KIT_INFORMATION.sql',
            kit, ', '.join([str(x) for x in serials])
        )

        insert_into_postgres_table(
            POSTGRES_CURSOR,
            KITS_TO_TRACK_TABLE,
            nki_headers, nki_results
        )

    ktr_headers, ktr_results, ktr_f_data = f_results(ORACLE_CURSOR, 'ORACLE_KIT_TRACKER.sql')

    insert_into_postgres_table(
        POSTGRES_CURSOR,
        KIT_PROGRESS_TABLE,
        ktr_headers, ktr_results,
        truncate=True
    )

