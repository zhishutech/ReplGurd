#/usr/bin/python
#coding：utf8
#作者：吴炳锡 知数堂MySQL课程中复制故障自动化处理Demo

import sys
import os
import re
import optparse
import pymysql
from pymysql.constants import CLIENT

MYSQL_SHOW_SLAVE_STATUS     = 'SHOW SLAVE STATUS;'
GTID_MODE = "select @@gtid_mode"
com_mysqlbinlog = "/usr/local/mysql/bin/mysqlbinlog"

GET_FROM_LOG="%s -v --base64-output=decode-rows -R --host='%s' --port=%d --user='%s' --password='%s' --start-position=%d --stop-position=%d %s |grep @%s|head -n 1"


FLAGS = optparse.Values()
parser = optparse.OptionParser()


def DEFINE_string(name, default, description, short_name=None):
    if default is not None and default != '':
        description = "%s (default: %s)" % (description, default)
    args = [ "--%s" % name ]
    if short_name is not None:
        args.insert(0, "-%s" % short_name)

    parser.add_option(type="string", help=description, *args)
    parser.set_default(name, default)
    setattr(FLAGS, name, default)

def DEFINE_integer(name, default, description, short_name=None):
    if default is not None and default != '':
        description = "%s (default: %s)" % (description, default)
    args = [ "--%s" % name ]
    if short_name is not None:
        args.insert(0, "-%s" % short_name)

    parser.add_option(type="int", help=description, *args)
    parser.set_default(name, default)
    setattr(FLAGS, name, default)

DEFINE_integer('db_port', '3306', 'DB port : 3306')
DEFINE_string('db_user', 'wubx', 'DB user ')
DEFINE_string('db_password', '', 'DB password')
DEFINE_string('db_host', '127.0.0.1', 'DB  Hostname')


def ShowUsage():
    print("python RplGurad.py --db_host=192.168.11.111 --db_port=3310 --db_user='wubx' --db_password='wubxwubx'")
    parser.print_help()
    exit(1)
    
def ParseArgs(argv):
    usage = sys.modules["__main__"].__doc__
    parser.set_usage(usage)
    unused_flags, new_argv = parser.parse_args(args=argv, values=FLAGS)
    return new_argv

def get_conn():
    return pymysql.connect(host=FLAGS.db_host, port=int(FLAGS.db_port), user=FLAGS.db_user,passwd=FLAGS.db_password,
                           client_flag=CLIENT.MULTI_STATEMENTS)

def start_slave():
    cursor = conn.cursor()
    cursor.execute("start slave")
    cursor.close()

def get_tb_pk(db_table):
    db, tb = db_table.split('.')
    conn = get_conn()
    sql = "select column_name,ordinal_position from information_schema.columns where table_schema='%s' and table_name='%s' and column_key='PRI';" % (db, tb)
    cursor = conn.cursor()
    cursor.execute(sql)
    r = cursor.fetchone()
    cursor.close()
    conn.close()
    return r


def get_rpl_worker(conn):
    cursor = conn.cursor()
    cursor.execute("select @@slave_parallel_workers")
    r = cursor.fetchone()
    cursor.close()
    if r[0] >= 1:
        return 1
    else:
        return 0

    return 0

def get_rpl_mode(conn):
    cursor = conn.cursor()
    cursor.execute(GTID_MODE)
    r = cursor.fetchone()
    cursor.close()
    
    if (r[0] == "ON"):
        return 1
    else:
        return 0

def handler_multi_1062(r):
    conn = get_conn()
    cursor = conn.cursor()
    cursor.execute("select LAST_ERROR_MESSAGE from performance_schema.replication_applier_status_by_worker where "
                   "LAST_ERROR_NUMBER=1062 limit 1")
    emsg = cursor.fetchone()[0]
    print(emsg)
    pattern = r"Could not execute Write_rows event on table (?P<tb>\w+.\w+); Duplicate entry '(?P<pk>\d+)' for key"
    regex = re.compile(pattern)
    match = regex.search(emsg)
    print(match.groupdict())
    pk_col = get_tb_pk(match['tb'])[0]
    baksql = "select * from %s where %s=%s" %(match['tb'], pk_col, match['pk'])
    print(baksql)
    delsql = "delete from %s where %s=%s" %(match['tb'], pk_col, match['pk'])
    conn = get_conn()
    cursor = conn.cursor()
    cursor.execute(baksql)
    print(cursor.fetchone())
    conn.commit()
    print(delsql)
    cursor.execute("set session sql_log_bin=0;")
    cursor.execute(delsql)
    #cursor.execute("set  session sql_log_bin=1")
    cursor.execute("start slave sql_thread")
    cursor.close()
    conn.commit()
    conn.close()
    return 0

def handler_multi_1032(r):
    conn = get_conn()
    cursor = conn.cursor()
    cursor.execute("select LAST_ERROR_MESSAGE from performance_schema.replication_applier_status_by_worker where "
                   "LAST_ERROR_NUMBER=1032 limit 1")
    emsg = cursor.fetchone()[0]

    print(emsg)
    patterns = [
        r'(?P<uuid>[0-9a-f]{8}(-[0-9a-f]{4}){3}-[0-9a-f]{12})\:(?P<gtid>\d+)',
        r'(?P<binlog>(\w+\-\w+\.\d+)), end_log_pos (?P<pos>(\d+));',
        r'Could not execute (?P<event>\w+)_rows event on table (?P<db>\w+).(?P<tb>\w+)',
    ]
    d = {}
    for pattern in patterns:
        regex = re.compile(pattern)
        match = regex.search(emsg)
        d.update(match.groupdict())
    print(d)

    if (d['event'].upper() == 'DELETE'):
        sql = "stop slave sql_thread; set gtid_next='%s:%s'; begin;commit; set gtid_next=AUTOMATIC;start slave " \
              "sql_thread;" % (d['uuid'], d['gtid'])
        print(sql)
        cursor.execute(sql)

    if (d['event'].upper() == 'UPDATE' ):
        dbtable="%s.%s"%(d['db'], d['tb'])
        pk_seq = get_tb_pk(dbtable)[1]
        do_getlog = GET_FROM_LOG % (
        com_mysqlbinlog, r['Master_Host'], int(r['Master_Port']), FLAGS.db_user, FLAGS.db_password,
        int(r['Exec_Master_Log_Pos']), int(d['pos']), d['binlog'], pk_seq)
        print(do_getlog)
        pk_value = os.popen(do_getlog).readlines()[0].split("=", 2)[1].rstrip()
        print(pk_value)
        sql = mk_tb_replace(dbtable, pk_value, pk_seq)
        cursor.execute("set session sql_log_bin=0;")
        cursor.execute(sql)
        # cursor.execute("set  session sql_log_bin=1")
        cursor.execute("start slave sql_thread")
        conn.commit()
        cursor.close()
    conn.close()
    return 0


    

def mk_tb_replace(db_table, pk_value, pk_seq):
    db, tb_name = db_table.split(".")
    r = "replace into %s.%s "%(db,tb_name)
    
    sql = "select column_name, ORDINAL_POSITION from information_schema.columns where table_schema='%s' and table_name='%s' and IS_NULLABLE='NO';" % (db, tb_name)
    #print sql
    
    col_list=''
    value_list=''
    
    conn = get_conn()
    cusror = conn.cursor()
    cusror.execute(sql)
    result = cusror.fetchall()
    for col in result:
        if (col[1] == pk_seq):
            col_list = col_list +"%s," % (col[0])
            value_list = value_list + "'%s'," % (pk_value)
        else:
            col_list = col_list +"%s," % (col[0])
            value_list = value_list + "'%s'," % ('1')
    print(value_list)
    r = r+"(%s) values(%s)" % ( col_list.rstrip(','), value_list.rstrip(','))
    cusror.close()
    conn.close()
    return r.rstrip(',')
        


def get_slave_status(conn):
    cursor = conn.cursor(pymysql.cursors.DictCursor)
    cursor.execute(MYSQL_SHOW_SLAVE_STATUS)
    result = cursor.fetchone()
    return result
    cursor.close()

if __name__ == '__main__':
        ParseArgs(sys.argv[1:])
        conn = get_conn()
        rpl_mode = get_rpl_mode(conn)
        slave_parallel = get_rpl_worker(conn)

        if (rpl_mode == 0 ):
            print("只支持GTID模式的复制故障修复")
            exit(1)

        if (slave_parallel == 0 ):
            print("只支持slave_parallel_workers大于0的复制模式修复")

        r = get_slave_status(conn)
        #print(r)
        if (r['Slave_IO_Running'].upper() == "YES" and r['Slave_SQL_Running'].upper() == "YES"):
            print("Replication OK")
            if (r['Seconds_Behind_Master'] > 0):
                print(r['Seconds_Behind_Master'])
                
            conn.close()
            exit(0)

        while( 1 ):
            r = get_slave_status(conn)
            if (r['Slave_IO_Running'] == "Yes" and r['Slave_SQL_Running'] == "No"):
                    if (r['Last_Errno'] == 1032 ):
                        r1032 = handler_multi_1032(r)
                    if (r['Last_Errno'] == 1062):
                        r1062 = handler_multi_1062(r)

            elif (r['Slave_IO_Running'] == "No" and r['Slave_SQL_Running'] == "Yes"):
                print("Replication slave： IO_thread no running ; do start slave")
                start_slave(conn)
                exit(1)
            else:
                exit(0)

        conn.close()
