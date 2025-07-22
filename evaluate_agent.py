import requests
from CreateSessionRequest import CreateSessionRequest
from SessionResponse import SessionResponse
from trino.dbapi import connect
from ordered_set import OrderedSet

from enum import Enum


class TestResult(Enum):
    PASS = 1
    FAIL = 2
    INVALID_GOLD = 3
    INVALID_TEST = 4


class StarburstText2SqlSession:

    def __init__(
            self,
            server_host: str,
            server_port: int,
            username: str,
            password: str,
            schema: str,
            catalog: str = 'hive',
            role: str = "sysadmin",
            proto: str = 'https',
            model_id: str = 'gpt4o',

    ):
        self.server_host = server_host
        self.server_port = server_port
        self.username = username
        self.password = password
        self.proto = proto
        self.model_id = model_id
        self.http_url = f'{proto}://{server_host}:{server_port}/ui/api/agent/sessions'
        self.role = role
        self.cookie = None
        self.conn = connect(
            host=server_host,
            port=server_port,
            user=username,
            catalog=catalog,
            schema=schema,
        )

    def __get_ui_cookie__(self):
        if self.cookie is None:
            form_body = f'username={self.username}'
            response = requests.post(
                url=f'{self.proto}://{self.server_host}:{self.server_port}/ui/login',
                data=form_body,
                headers={'Content-Type': 'application/x-www-form-urlencoded'},
                allow_redirects=False
            )
            if not response.ok:
                raise Exception('login failed: ' + response.text)
            self.cookie = response.cookies.get('Trino-UI-Token')
            if self.cookie is None:
                print('No cookie!')
                for cookie in response.cookies.get_dict():
                    print(cookie)
                    raise Exception('need cookie')
        return self.cookie

    def compare_sql(self, test_sql, gold_standard_sql) -> TestResult:
        cur = self.conn.cursor()
        gold_standard_sql = self.__fixup_spider_sql__(gold_standard_sql)
        try:
            cur.execute(test_sql)
            test_rows = cur.fetchall()
        except Exception as e:
            print('Invalid test query: ' + test_sql)
            print(e)
            return TestResult.INVALID_TEST

        try:
            cur.execute(gold_standard_sql)
            gold_rows = cur.fetchall()
        except Exception as e:
            print('Invalid gold query: ' + gold_standard_sql)
            print(e)
            return TestResult.INVALID_GOLD
        if 'ORDER' in test_sql or 'ORDER' in gold_standard_sql:
            is_equal = self.compare_sorted(gold_rows, test_rows)
        else:
            is_equal = self.compare_unsorted(gold_rows, test_rows)

        if is_equal == TestResult.FAIL:
            print('Results not equal')
            print('Test sql: ' + test_sql)
            print('Gold sql: ' + gold_standard_sql)
            print('Test sql rows:')
            for row in test_rows:
                print(', '.join(map(lambda el: str(el), row)))
            print('Gold sql data:')
            for row in gold_rows:
                print(', '.join(map(lambda el: str(el), row)))
        return is_equal

    def compare_sorted(self, gold_rows: list, test_rows: list) -> TestResult:
        if test_rows == gold_rows:
            return TestResult.PASS
        if (list(set([','.join(map(lambda el: str(el), row)) for row in test_rows]))
                == [','.join(map(lambda el: str(el), row)) for row in gold_rows]):
            print('Rows equal after applying distinct to gold')
            return TestResult.PASS
        # PASS extra columns
        if (len(gold_rows) > 0 and (type(gold_rows[0]) is list and len(gold_rows[0])) == 1 and len(test_rows) > 0 and
                (type(test_rows[0]) is list and len(test_rows[0]) > 1)):
            for i in range(len(test_rows[0])):
                is_column_equal = self.compare_sorted(gold_rows, [[row[i]] for row in test_rows])
                if is_column_equal:
                    print('Correct result, contains extra columns')
                    return TestResult.PASS
        # PASS two columns in different order
        if (len(gold_rows) > 0 and (type(gold_rows[0]) is list and len(gold_rows[0])) == 2 and len(test_rows) > 0 and
                (type(test_rows[0]) is list and len(test_rows[0]) == 2)):
            if gold_rows == [[row[1], row[0]] for row in test_rows]:
                print('Correct result, columns in different order (sorted)')
                return TestResult.PASS
        deduped_gold = list(OrderedSet([','.join(map(lambda el: str(el), row)) for row in gold_rows]))
        if (len(deduped_gold) == len(test_rows) and
                deduped_gold == [','.join(map(lambda el: str(el), row)) for row in test_rows]):
            print('Rows equal after applying distinct to gold')
            return TestResult.PASS
        return TestResult.FAIL

    def compare_unsorted(self, gold_rows, test_rows) -> TestResult:
        if (len(test_rows) == len(gold_rows)
                and (set([','.join(map(lambda el: str(el), row)) for row in gold_rows])
                     == set([','.join(map(lambda el: str(el), row)) for row in test_rows]))):
            return TestResult.PASS
        if len(gold_rows) > 0 and len(gold_rows[0]) == 1 and len(test_rows) > 0 and len(test_rows[0]) > 1:
            for i in range(len(test_rows[0])):
                is_column_equal = self.compare_unsorted(gold_rows, [[row[i]] for row in test_rows])
                if is_column_equal:
                    print('Correct result, contains extra columns')
                    return TestResult.PASS
        deduped_gold = set([','.join(map(lambda el: str(el), row)) for row in gold_rows])
        if (len(deduped_gold) == len(test_rows) and
                deduped_gold == set([','.join(map(lambda el: str(el), row)) for row in test_rows])):
            print('Rows equal after applying distinct to gold')
            return TestResult.PASS
        if (len(gold_rows) > 0 and (type(gold_rows[0]) is list and len(gold_rows[0])) == 2 and len(test_rows) > 0 and
                (type(test_rows[0]) is list and len(test_rows[0]) == 2)):
            if (set([','.join(map(lambda el: str(el), row)) for row in gold_rows])
                    == set([','.join([str(row[1]), str(row[0])]) for row in test_rows])):
                print('Correct result, columns in different order')
                return TestResult.PASS

        return TestResult.FAIL

    def __fixup_spider_sql__(self, sql: str):
        return sql.replace('"', "'")  #sqllite uses "" for string literals

    def get_sql(self, question: str, data_product_id: str) -> str:
        create_session_request = CreateSessionRequest(rawQuestion=question, dataProductId=data_product_id)

        response = requests.post(
            url=f'{self.http_url}/{self.model_id}',
            json=create_session_request.asdict(),
            headers={
                "Content-Type": "application/json",
                "Trino-UI-Token": self.__get_ui_cookie__(),
                #                "x-trino-role": f"system=ROLE{{{self.role}}}",
                "x-trino-user": self.username
            }
        )
        if not response.ok or response.status_code in [303, 302]:
            print(response.text)
            raise Exception('Request failed! Reason: ' + response.reason)
        session = SessionResponse.load(response.json())

        return session.generateQueryResponse.query


if __name__ == "__main__":
    import json

    test_db_id = 'car_1'
    tests = {
        'car_1': {'data_product_id': '0fa7a325-74b5-44a0-822b-04671d7b7113', 'schema': 'car'},
        'flight_2': {'data_product_id': '08a0d55d-460e-49b9-816a-63d841092a31', 'schema': 'flights'}
    }

    test_data_product_id = tests[test_db_id]['data_product_id']
    dev = json.load(open('./evaluation_examples/examples/dev.json'))
    flight_question = [test for test in dev if test['db_id'] == test_db_id]
    sb = StarburstText2SqlSession(
        'localhost',
        9090,
        username='admin',
        password='',
        proto='http',
        schema=tests[test_db_id]['schema'])

    passing = 0
    total_tests = 0
    for test in flight_question:
        q = sb.get_sql(f'According to the data, {test["question"]}', test_data_product_id)
        success = sb.compare_sql(q, test["query"])
        if success == TestResult.PASS:
            passing += 1
            total_tests += 1
        elif success != TestResult.INVALID_GOLD:
            total_tests += 1
            print('Question: ' + test["question"])

    print(str(passing) + ' correct queries out of ' + str(total_tests))
