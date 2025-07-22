import json
import argparse
import trino
from trino.auth import BasicAuthentication
import sys


def execute_query(conn, query):
    """
    Executes a SQL query using the provided Trino connection and prints results.
    """
    try:
        cur = conn.cursor()
        cur.execute(query)
        rows = cur.fetchall()

        # Print column names (if available)
        if cur.description:
            column_names = [col[0] for col in cur.description]
            print(" | ".join(column_names))
            print("-" * (sum(len(col[0]) for col in cur.description) + (len(cur.description) - 1) * 3)) # Simple separator

        # Print rows
        for row in rows:
            print(" | ".join(map(str, row)))

    except trino.exceptions.TrinoUserError as e:
        print(f"Error executing query: {e}", file=sys.stderr)
        sys.exit(1)
    except Exception as e:
        print(f"An unexpected error occurred during query execution: {e}", file=sys.stderr)
        sys.exit(1)
    finally:
        if conn:
            conn.close()


def create_statements(conn: trino.dbapi.Connection , output_file: str | None):
    tables = json.load(open('./evaluation_examples/examples/tables.json'))

    sqlite_type_to_trino_type = {"number": "decimal(10,3)", "text": "varchar"}
    create_table_statements = {}
    create_view_statements = {}
    drop_table_statements = {}

    catalog = 'hive'
    db_with_csv = ['car_1', 'flight_2']
    with_statement = "WITH (format = 'CSV', skip_header_line_count=1, csv_quote = '''')"

    for db in [table for table in tables if table['db_id'] in db_with_csv]:
        table_columns = {name: [] for name in db['table_names_original']}
        view_columns = {name: [] for name in db['table_names_original']}

        for i, (table_index, column_name) in enumerate(db['column_names_original']):
            # todo: figure out why some of the column lists contain a '*'
            if column_name == '*' or table_index < 0:
                continue
            (view_columns[db['table_names_original'][table_index]]
             .append(
                f'CAST(trim("{column_name}") AS {"decimal(10)" if "id" in column_name.lower() else sqlite_type_to_trino_type[db["column_types"][i]]}) "{column_name}"'))

            (table_columns[db['table_names_original'][table_index]]
             .append(f'"{column_name}" varchar'))

        create_table_statements[db["db_id"]] = [
            f"CREATE TABLE {catalog}.{db['db_id']}.{table_name} ({', '.join(table_columns[table_name])}) {with_statement}"
            for table_name in db['table_names_original']]

        create_view_statements[db["db_id"]] = [f"""
CREATE OR REPLACE VIEW {catalog}.{db['db_id']}.{table_name}_vw 
AS SELECT {', '.join(view_columns[table_name])}
FROM {catalog}.{db['db_id']}.{table_name}""" for table_name in db['table_names_original']]
        drop_table_statements[db["db_id"]] = [f"DROP TABLE IF EXISTS {catalog}.{db['db_id']}.{table_name}"
                                              for table_name in db['table_names_original']]
    if (output_file):
        f = open(output_file, 'w')
    else:
        f = sys.stdout

    cursor = conn.cursor()
    for db in drop_table_statements.values():
        for dt in db:
            cursor.execute(dt)
            cursor.fetchall()
            f.write(dt + ';\n')

    for db in create_table_statements.values():
        for ct in db:
            cursor.execute(ct)
            cursor.fetchall()
            f.write(ct + ';\n')

    for db in create_view_statements.values():
        for cv in db:
            cursor.execute(cv)
            cursor.fetchall()
            f.write(cv + ';\n')


def initialize_trino_client(args):
    """
    Initializes a Trino client connection based on provided arguments.
    """
    auth_method = None
    if args.auth_type == 'basic':
        if not args.auth_user or not args.auth_password:
            print("Error: --auth-user and --auth-password are required for basic authentication.", file=sys.stderr)
            sys.exit(1)
        auth_method = BasicAuthentication(args.auth_user, args.auth_password)
    elif args.auth_type != 'none':
        print(f"Warning: Authentication type '{args.auth_type}' is not fully implemented in this example. Proceeding without specific auth.", file=sys.stderr)
        # You would extend this section to handle other authentication types
        # like Kerberos, JWT, etc., using appropriate classes from trino.auth

    try:
        conn = trino.dbapi.connect(
            host=args.host,
            port=args.port,
            user=args.user,
            catalog=args.catalog,
            schema=args.schema,
            http_headers={'X-Trino-Client-Info': 'Python CLI Client'},
            verify=args.ssl,  # True for SSL verification, False to skip (not recommended for production)
            auth=auth_method
        )
        return conn
    except trino.exceptions.TrinoConnectionError as e:
        print(f"Error connecting to Trino: {e}", file=sys.stderr)
        sys.exit(1)
    except Exception as e:
        print(f"An unexpected error occurred during connection: {e}", file=sys.stderr)
        sys.exit(1)


def main():
    parser = argparse.ArgumentParser(
        description="Initialize a Trino Python client and execute a SQL query."
    )

    # Required arguments
    parser.add_argument('--host', required=True, help='Trino server hostname or IP address.')
    parser.add_argument('--port', type=int, required=True, help='Trino server port.')
    parser.add_argument('--user', required=True, help='User for Trino authentication.')
    parser.add_argument('--catalog', required=True, help='Trino catalog to use (e.g., hive, tpch).')
    parser.add_argument('--schema', required=True, help='Trino schema to use (e.g., default, sf1).')
    parser.add_argument('--query', required=True, help='SQL query to execute.')

    # Optional arguments
    parser.add_argument('--ssl', action='store_true', help='Use SSL for connection (HTTPS).')
    parser.add_argument(
        '--auth-type',
        choices=['none', 'basic'], # Extend with 'kerberos', 'jwt', etc. as needed
        default='none',
        help='Authentication type (default: none). "basic" requires --auth-user and --auth-password.'
    )
    parser.add_argument('--auth-user', help='Username for basic authentication.')
    parser.add_argument('--auth-password', help='Password for basic authentication.')

    args = parser.parse_args()

    conn = initialize_trino_client(args)


if __name__ == '__main__':
    import sys

    if len(sys.argv) > 1:
        create_statements(sys.argv[1])
    else:
        create_statements(None)
