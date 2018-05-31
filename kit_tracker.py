import cx_Oracle
import psycopg2
import argparse
import copy
import json
import re
import os
import sys
from colorama import init, Fore
from settings import \
    ORACLE_CONNECTION_STRING, \
    PORSTGRES_CONNECTION_STRING, \
    NEW_KIT_REGEX_PATTERN, \
    KITS_TO_TRACK_TABLE, \
    KIT_PROGRESS_TABLE, \
    TRUNCATE_TABLE, \
    DEFAULT_VERBOSITY_LEVEL, \
    MAX_VERBOSITY_LEVEL, \
    Verbosity

init()


def check_files_exists(*files):
    failed = []
    for file in files:
        if not os.path.isfile(file):
            print(Fore.RED + 'could not locate file [' + Fore.WHITE + file + Fore.RED + ']')
            failed.append(False)
        failed.append(True)
    return all(failed)


class Application:
    def __init__(self, verbose=False, max_verbose=DEFAULT_VERBOSITY_LEVEL, auto_connect=True):
        self.verbose = verbose
        self.max_verbose = max_verbose

        self.__ORACLE_CONNECTION = None
        self.__POSTGRES_CONNECTION = None
        self.__ORACLE_CURSOR = None
        self.__POSTGRES_CURSOR = None

        if auto_connect:
            self.connect()

    def __verbose_print(self, text, enum=Verbosity.GENERAL, color=Fore.LIGHTWHITE_EX):
        if self.verbose:
            if enum == Verbosity.GENERAL and Verbosity.GENERAL.value <= self.max_verbose:
                print(Fore.LIGHTWHITE_EX + str(text))
            elif enum == Verbosity.INFO and Verbosity.INFO.value <= self.max_verbose:
                print(Fore.LIGHTCYAN_EX + str(text))
            elif enum == Verbosity.WARNING and Verbosity.WARNING.value <= self.max_verbose:
                print(Fore.LIGHTYELLOW_EX + str(text))
            elif enum == Verbosity.ERROR and Verbosity.ERROR.value <= self.max_verbose:
                print(Fore.LIGHTRED_EX + str(text))
            elif enum == Verbosity.OTHER and Verbosity.OTHER.value <= self.max_verbose:
                print(color + str(text))
            else:
                print(Fore.WHITE + str(text))

    def connect(self):
        try:
            self.__POSTGRES_CONNECTION = psycopg2.connect(PORSTGRES_CONNECTION_STRING)
            self.__POSTGRES_CURSOR = self.__POSTGRES_CONNECTION.cursor()
        except psycopg2.Error as pgerror:
            m = '[{}] {}. Unable to connect to postgres'.format(pgerror.pgcode, pgerror)
            self.__verbose_print(m, enum=Verbosity.ERROR)

        try:
            self.__ORACLE_CONNECTION = cx_Oracle.connect(ORACLE_CONNECTION_STRING)
            self.__ORACLE_CURSOR = self.__ORACLE_CONNECTION.cursor()
        except cx_Oracle.Error as oraerror:
            m = '[{}]. Unable to connect to oracle.'.format(oraerror)
            self.__verbose_print(m, enum=Verbosity.ERROR)

    def test_connection(self, postgres_connection_string=PORSTGRES_CONNECTION_STRING,
                        oracle_connection_string=ORACLE_CONNECTION_STRING):
        pg_pass = False
        or_pass = False

        error_log = []

        self.__verbose_print('Testing database connections.', enum=Verbosity.INFO)

        try:
            pgc = psycopg2.connect(postgres_connection_string)
            pgc.close()
            pg_pass = True
        except psycopg2.Error as pgerror:
            m = '[{}] {}. Unable to connect to postgres'.format(pgerror.pgcode, pgerror)
            self.__verbose_print(m, enum=Verbosity.ERROR)
            error_log.append(m + '\n')

        try:
            orac = cx_Oracle.connect(oracle_connection_string)
            orac.close()
            or_pass = True
        except cx_Oracle.Error as oraerror:
            m = '[{}]. Unable to connect to oracle.'.format(oraerror)
            error_log.append(m + '\n')
            self.__verbose_print(m, enum=Verbosity.ERROR)

        if not all([pg_pass, or_pass]):
            self.__verbose_print('One of the two connections have failed and the application can not continue.\n '
                                 'Please resolve these issues before continuing.',
                                 enum=Verbosity.WARNING)
            for i in error_log:
                self.__verbose_print(i, enum=Verbosity.ERROR)
        else:
            self.__verbose_print('Connections to Postgres and Oracle successful!', enum=Verbosity.OTHER,
                                 color=Fore.LIGHTGREEN_EX)

    def force_truncate_tables(self):
        tables = [
            KIT_PROGRESS_TABLE,
            KITS_TO_TRACK_TABLE
        ]

        affected_tables = 0

        self.__verbose_print("Begin truncating tables")

        for t in tables:
            stmt = TRUNCATE_TABLE(t)

            self.__POSTGRES_CURSOR.execute('SAVEPOINT sp1')
            self.__verbose_print('Truncating table [{}]'.format(t), enum=Verbosity.INFO)

            try:
                self.__POSTGRES_CURSOR.execute(stmt)
            except psycopg2.Error as pgerror:
                m = '[{}] {}. Unable to truncate table [{}]'.format(
                    pgerror.pgcode, pgerror, t
                )
                self.__verbose_print(m, enum=Verbosity.ERROR)
                self.__POSTGRES_CURSOR.execute("ROLLBACK TO SAVEPOINT sp1")
                self.__verbose_print("Rolling back to savepoint sp1", enum=Verbosity.WARNING)
            else:
                self.__verbose_print(stmt, enum=Verbosity.INFO)
                self.__verbose_print('Releasing savepoint sp1', enum=Verbosity.INFO)
                self.__POSTGRES_CURSOR.execute('RELEASE SAVEPOINT sp1')
                affected_tables += 1

        self.__verbose_print('Committing transaction.', enum=Verbosity.INFO)
        self.__POSTGRES_CONNECTION.commit()
        self.__verbose_print("Truncation complete. [{}] tables affected.".format(str(affected_tables)),
                             enum=Verbosity.OTHER, color=Fore.LIGHTGREEN_EX)

    @staticmethod
    def load_query_from_file(file, *additional_args):
        with open(file, 'r')as qf:
            q = qf.read()
            if len(additional_args) > 0:
                return q.format(*additional_args)
            return q

    @staticmethod
    def remove_duplicates(data, index=0):
        values = []
        n_data = []
        for row in data:
            if row[index] not in values:
                values.append(row[index])
                n_data.append(row)
            else:
                pass
        return n_data

    def __f_results(self, cur, file, *additional_args, index=None):
        fd = []
        data = cur.execute(self.load_query_from_file(file, *additional_args))
        headers = tuple(i[0] for i in cur.description)
        if index is None:
            results = [row for row in data]
        else:
            results = self.remove_duplicates([row for row in data], index=9)
        for row in results:
            fd.append(
                {i[0]: i[1] for i in list(zip(headers, row))}
            )
        return headers, results, fd

    def __insert_into_postgres_table(self, connection, cursor, table_name, table_headers, data, truncate=False):
        inserted_rows = 0

        self.__verbose_print('Inserting [{}] rows into table [{}]'.format(str(len(data)), table_name),
                             enum=Verbosity.INFO)

        if truncate:
            self.__verbose_print('Truncating table [{}]'.format(table_name), enum=Verbosity.OTHER, color=Fore.BLUE)
            cursor.execute(TRUNCATE_TABLE(table_name))

        for row in data:
            stmt = r"INSERT INTO {}{} VALUES {}".format(
                table_name, str(tuple(table_headers)).replace("'", ''), str(tuple(row))
            )

            cursor.execute('SAVEPOINT sp1')

            try:
                cursor.execute(stmt)
            except Exception as error:
                self.__verbose_print(error, enum=Verbosity.ERROR)
                self.__verbose_print(cursor.statusmessage, enum=Verbosity.ERROR)
                self.__verbose_print('Rolling back transaction to savepoint sp1', enum=Verbosity.WARNING)
                cursor.execute("ROLLBACK TO SAVEPOINT sp1")
            except psycopg2.IntegrityError:
                self.__verbose_print('Rolling back transaction to savepoint sp1', enum=Verbosity.WARNING)
                cursor.execute("ROLLBACK TO SAVEPOINT sp1")
            else:
                self.__verbose_print(stmt, enum=Verbosity.INFO)
                inserted_rows += 1
                cursor.execute('RELEASE SAVEPOINT sp1')

        self.__verbose_print(
            'Committing database changes. [{}] rows of [{}] queued rows inserted into table [{}]'.format(
                str(inserted_rows), str(len(data)), table_name
            ), enum=Verbosity.OTHER, color=Fore.LIGHTGREEN_EX)

        connection.commit()

    @staticmethod
    def load_j_kit_data(json_file):
        with open(json_file, 'r')as jf:
            j_data = json.load(jf)

        return j_data['data']

    def update_new_kit_data_file(self, new_kit_file, json_file):
        nkf_file = new_kit_file
        jkf_file = json_file

        self.__verbose_print('Loading new kit data file [{}] and json data file [{}]'.format(
            new_kit_file, json_file
        ), enum=Verbosity.INFO)

        with open(nkf_file, 'r')as f:
            n_data = f.readlines()

        with open(jkf_file, 'r')as jf:
            j_data = json.load(jf)

        n_data = [x.replace('\n', '') for x in n_data]
        n_kits = {}
        j_kits = copy.deepcopy(j_data['data'])

        self.__verbose_print('Formatting new data')

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

        self.__verbose_print('Complete.', enum=Verbosity.INFO)

        self.__verbose_print('Creating new json data')

        data = copy.deepcopy(j_kits)
        kits = sorted(list(data.keys()))
        serials = sorted([int(x) for x in set(sum(data.values(), []))])

        self.__verbose_print('Complete.', enum=Verbosity.INFO)

        self.__verbose_print('Writing new json data to json file [{}]'.format(jkf_file), enum=Verbosity.INFO)

        with open(jkf_file, 'w')as jf:
            json.dump(
                {'data': data, 'kits': kits, 'serials': serials},
                jf, indent=4, ensure_ascii=True
            )

        return j_kits

    def main(self, new_kit_data):

        self.__verbose_print('Collecting data on kits from new kit data file '
                             'and uploading to postgres.', enum=Verbosity.INFO)

        for kit, serials in new_kit_data.items():
            nki_headers, nki_results, _ = self.__f_results(
                self.__ORACLE_CURSOR,
                'ORACLE_KIT_INFORMATION.sql',
                kit, ', '.join([str(x) for x in serials])
            )

            self.__insert_into_postgres_table(
                self.__POSTGRES_CONNECTION,
                self.__POSTGRES_CURSOR,
                KITS_TO_TRACK_TABLE,
                nki_headers, nki_results
            )

        self.__verbose_print('Complete.', enum=Verbosity.OTHER, color=Fore.LIGHTGREEN_EX)
        self.__verbose_print('Building kit tracker data from oracle', enum=Verbosity.INFO)

        ktr_headers, ktr_results, ktr_f_data = self.__f_results(self.__ORACLE_CURSOR, 'ORACLE_KIT_TRACKER.sql')

        self.__verbose_print('Uploading new kit tracker data to postgres', enum=Verbosity.INFO)

        self.__insert_into_postgres_table(
            self.__POSTGRES_CONNECTION,
            self.__POSTGRES_CURSOR,
            KIT_PROGRESS_TABLE,
            ktr_headers, ktr_results,
            truncate=True
        )

        self.__verbose_print('Complete.', enum=Verbosity.OTHER, color=Fore.LIGHTGREEN_EX)


if __name__ == '__main__':

    parser = argparse.ArgumentParser()

    parser.add_argument('--run', '-r',
                        action='store_true',
                        help='Run full script.'
                        )

    parser.add_argument('--update-kit-data', '-ukd', nargs=2,
                        metavar=('NEW_KIT_FILE', 'JSON_KIT_FILE'),
                        help='Update kit list with new list of kits')

    parser.add_argument('--verbose', '-v', nargs='?',
                        const=DEFAULT_VERBOSITY_LEVEL, type=int,
                        metavar='1 to ' + str(MAX_VERBOSITY_LEVEL),
                        help='Display information during programs execution'
                        )

    parser.add_argument('--test-connection', '-t',
                        action='store_true',
                        help='Test connection to databases.'
                        )

    parser.add_argument('--truncate-tables', '-tr', action='store_true',
                        help="Forcibly truncate tables in postgres"
                        )

    args = parser.parse_args()

    app = Application()

    if args.verbose:

        if args.verbose in range(1, MAX_VERBOSITY_LEVEL + 1):
            app.verbose = True
            app.max_verbose = args.verbose

            print(Fore.WHITE + 'Verbosity Level set to [' + Fore.LIGHTGREEN_EX + str(args.verbose) + Fore.WHITE + ']')
        else:
            print(Fore.WHITE + 'Max possible verbosity level is [' + Fore.LIGHTRED_EX + str(
                MAX_VERBOSITY_LEVEL) + Fore.WHITE + ']')
            sys.exit()

    if args.test_connection:
        app.test_connection()

    if args.update_kit_data:
        nkf = args.update_kit_data[0]
        jkf = args.update_kit_data[1]

        if check_files_exists(nkf, jkf):
            app.update_new_kit_data_file(nkf, jkf)

    if args.truncate_tables:
        app.force_truncate_tables()

    if args.run:
        if check_files_exists('3DAY_NEW_KIT_STOCK.json'):
            app.main(
                app.load_j_kit_data('3DAY_NEW_KIT_STOCK.json')
            )
