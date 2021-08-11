import pymonetdb
import json
import os
import sys
import datetime
import argparse

host = os.getenv('DB_HOST','monetdb.monetdb')
port = os.getenv('DB_PORT', 50000)
db = os.getenv('DB', 'demo')
schema = os.getenv('SCHEMA', 'kooplex')

def db_exec(statement, transaction, fetch):
    try:
        t0 = datetime.datetime.now()
        print ("{0} SQL: {1}".format(t0, statement), file = sys.stderr)
        cur = None
        cur = myConnection.cursor()
        cur.execute( statement )
        response = cur.fetchall() if fetch else None
        if transaction:
            myConnection.commit()
            print ("commited", file = sys.stderr)
    except Exception as e:
        print ("ERROR: {}".format(e), file = sys.stderr)
        if transaction:
            myConnection.rollback()
            print ("rolled back", file = sys.stderr)
    finally:
        if cur:
            cur.close()
        t1 = datetime.datetime.now()
        print ("{0} the duration of running statement {1}".format(t1, t1 - t0), file = sys.stderr)
        if response:
            for r in response:
                if 'event_ts' in r:
                    r['event_ts'] = r['event_ts'].isoformat()
        return response


def con(db = db):
    username = os.getenv('SECRET_USERNAME', 'monetdb')
    password = os.getenv('SECRET_PASSWORD', 'monetdb')
    
    return pymonetdb.connect(username=username, password=password, hostname=host, database=db, port=port)

if __name__ == '__main__':
## the table schema
## CREATE TABLE IF NOT EXISTS operation (
##      event_ts timestamp,
##      last_stage int,
##      last_exit_code int,
##      stage int,
##      exit_code int,
## 	extra_info text -- json encoded information
## );
    parser = argparse.ArgumentParser()
    subparsers = parser.add_subparsers(help = 'Choose a command')
    init_parser = subparsers.add_parser('init', help = '"init" help')
    init_parser.set_defaults(action = lambda: 'init')
    truncate_parser = subparsers.add_parser('truncate', help = '"truncate" help')
    truncate_parser.set_defaults(action = lambda: 'truncate')
    dump_parser = subparsers.add_parser('dump', help = '"dump" help')
    dump_parser.set_defaults(action = lambda: 'dump')
    get_parser = subparsers.add_parser('get', help = '"get" help')
    get_parser.set_defaults(action = lambda: 'get')
    append_parser = subparsers.add_parser('append', help = '"append" help')
    append_parser.set_defaults(action = lambda: 'append')
    append_parser.add_argument('-s', '--stage', required = True, type = int, 
            help = 'set new stage')
    append_parser.add_argument('-c', '--code', required = True, type = int, 
            help = 'set exit code')
    append_parser.add_argument('-e', '--extra', required = True, 
            help = 'set json serialized extra information')
    assert_parser = subparsers.add_parser('assert', help = '"assert" help')
    assert_parser.set_defaults(action = lambda: 'assert')
    assert_parser.add_argument('-s', '--stage', required = True, type = int, 
            help = 'make sure the current stage is matched')
    newrecords_parser = subparsers.add_parser('newrecords', help = '"newrecords" help')
    newrecords_parser.set_defaults(action = lambda: 'newrecords')
    newrecords_parser.add_argument('-t', '--source', required = True, choices = [ 'cov', 'vcf' ],
            help = 'get the number of new record files for given source')

    args = parser.parse_args()
    try:
        command = args.action()
    except:
        print ("make sure {0} is run with proper command line argiments".format(sys.argv[0]), file = sys.stderr)
        raise

    try:
        myConnection = con(db)
        print ("{0} connected to db {1}".format(datetime.datetime.now(), db), file = sys.stderr)
    
        db_exec("set schema kooplex", transaction=False)
        
        if command == 'init':
            resp = db_exec("SELECT * FROM operation", transaction = False, fetch = True)
            assert len(resp) == 0, "Table operation must be initialized already"
            db_exec("INSERT INTO operation (event_ts, last_stage, last_exit_code, stage, exit_code, extra_info) VALUES ('{0}', -1, -1, 0, 0, '{1}')".format(datetime.datetime.now(), {}), transaction = True, fetch = False)
    
        if command == 'truncate':
            db_exec("TRUNCATE TABLE operation", transaction = True, fetch = False)
    
        if command == 'append':
            json.loads(args.extra)
            resp = db_exec("SELECT stage, exit_code FROM operation ORDER BY event_ts DESC LIMIT 1", transaction = False, fetch = True)
            print (json.dumps(resp[0]))
            db_exec("INSERT INTO operation (event_ts, last_stage, last_exit_code, stage, exit_code, extra_info) VALUES ('{0}', {1}, {2}, {3}, {4}, '{5}')".format(
                datetime.datetime.now(), resp[0]['stage'], resp[0]['exit_code'], args.stage, args.code, args.extra
                ), transaction = True, fetch = False)
    
        if command == 'dump':
            print (db_exec("SELECT * FROM operation ORDER BY event_ts DESC", transaction = False, fetch = True))
    
        if command == 'get':
            resp = db_exec("SELECT * FROM operation ORDER BY event_ts DESC LIMIT 1", transaction = False, fetch = True)
            print (json.dumps(resp[0]))
    
        if command == 'assert':
            resp = db_exec("SELECT stage, exit_code FROM operation ORDER BY event_ts DESC LIMIT 1", transaction = False, fetch = True)
            assert resp[0]['exit_code'] == 0, 'Last command was not exited cleanly'
            assert resp[0]['stage'] == args.stage, 'Stage mismatch'

        if command == 'newrecords':
            resp = db_exec("SELECT stage, exit_code FROM operation ORDER BY event_ts DESC LIMIT 1", transaction = False, fetch = True)
            assert resp[0]['exit_code'] == 0, 'Last command was not exited cleanly'
            assert resp[0]['stage'] == 2, 'Stage mismatch'
            resp = db_exec("SELECT event_ts FROM operation WHERE exit_code = 0 AND stage = 1 ORDER BY event_ts DESC LIMIT 1", transaction = False, fetch = True)
            t0 = resp[0]['event_ts']
            resp = db_exec("SELECT * FROM operation WHERE exit_code = 0 AND stage = 2 AND event_ts > '{}'".format(t0), transaction = False, fetch = True)
            n = 0
            for r in resp:
                jsr = json.loads(r['extra_info'])
                if ('command' in jsr) and (args.source in jsr['command']):
                    n += jsr['n_files']
            print (n)
    
    finally:
        myConnection.close()
        print ("{} disconnected from db engine".format(datetime.datetime.now()), file = sys.stderr)
