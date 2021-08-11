import argparse
import datetime
import pymonetdb
import os
import sys

host = os.getenv('DB_HOST','monetdb.monetdb')
port = os.getenv('DB_PORT', 50000)
db = os.getenv('DB', 'demo')
schema = os.getenv('SCHEMA', 'kooplex')


def db_exec(myConnection, statement, transaction = False, fetch = True):
    try:
        t0 = datetime.datetime.now()
        print ("{0} SQL: {1}".format(t0, statement), file = sys.stderr)
        cur = None
        cur = myConnection.cursor()
        cur.execute( statement )
        if transaction:
            myConnection.commit()
            print ("commited", file = sys.stderr)
        if fetch:
            for r in cur.fetchall():
                print (r)
    except Exception as e:
        print ("ERROR: {}".format(e))
        if transaction:
            myConnection.rollback()
            print ("rolled back", file = sys.stderr)
    finally:
        if cur:
            cur.close()
        t1 = datetime.datetime.now()
        print ("{0} the duration of running statement {1} s".format(t1, t1 - t0), file = sys.stderr)


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument("-t", "--list_tables", action = "store_true", help = "list tables")
    parser.add_argument("-v", "--list_views", action = "store_true", help = "list views")
    parser.add_argument("-c", "--count", type = str, help = "count records in the table")
    parser.add_argument("-s", "--schema", type = str, help = "describe columns of the table")
    parser.add_argument("-S", "--top10", type = str, help = "select 10 records of the table")
    parser.add_argument("-l", "--locks", action = "store_true", help = "show running queries")
    parser.add_argument("-k", "--kill", type = int, help = "kill a pid")
    args = parser.parse_args()

    username = os.getenv('SECRET_USERNAME', 'monetdb')
    password = os.getenv('SECRET_PASSWORD', 'monetdb')
    
    con = pymonetdb.connect(username=username, password=password, hostname=host, database=db, port=port)

    print ("connected to db {0}".format(db), file = sys.stderr)
    try:

    
        if args.list_tables:
            statement = """
SELECT tables.name 
FROM sys.tables WHERE tables.system=false 
ORDER BY tables.name 
            """
            db_exec(con, statement, False, True)

        if args.list_views:
            statement = """
SELECT name FROM sys.tables WHERE type IN (SELECT table_type_id FROM sys.table_types WHERE table_type_name = 'VIEW')
            """
            db_exec(con, statement, False, True)
    
        if args.count:
            statement = """
SELECT COUNT(*)
FROM {}
            """.format(args.count)
            db_exec(con, statement, False, True)
    
        if args.schema:
            statement = """
SELECT tables.name, columns.name, columns.type 
FROM sys.tables join sys.columns on tables.id=columns.table_id
WHERE tables.name = '{}'
            """.format(args.schema)
            db_exec(con, statement, False, True)
    
        if args.top10:
            statement = """
SELECT * 
FROM {} LIMIT 10
            """.format(args.top10)
            db_exec(con, statement, False, True)
    
        if args.locks:
            statement = """
select * from sys.queue
            """
            db_exec(con, statement, False, True)

        if args.kill:
            statement = """
call sys.stop({})
            """.format(args.kill)
            db_exec(con, statement, True, True)
    
    finally:
        con.close()
        print ("connected to db {0}".format(db), file = sys.stderr)

