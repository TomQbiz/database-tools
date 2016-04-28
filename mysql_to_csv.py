#!/usr/bin/python
##########################
#
# Connect to lookout analytics database and get table descriptions from dim_ and xfm_ tables
#
# T.H 4/18/2016
#

import MySQLdb
import sys
import os
from ConfigParser import SafeConfigParser
import codecs
import sys
import re

from signal import signal, SIGPIPE, SIG_DFL
signal(SIGPIPE, SIG_DFL)

# Number of rows to read and write
block_size = 10000

# config for database access
config_file='lookout.ini'
config_section = "mysql lookout legacy analytics"

parser = SafeConfigParser()

#
# one parameter the sql code file
#
if len(sys.argv) < 2:
	print "Usage: " + sys.argv[0] + " table.sql" 
	exit(-1)

if os.path.isfile(sys.argv[1]):
	sqlfilename = sys.argv[1]

csvfilename = sqlfilename.strip(".sql")+".csv"

try:
	csv = open(csvfilename,"w+")
except:
	print "Cannot create " + csvfilename + "\n"
	exit(-1)

# Open the file with the correct encoding
with codecs.open(config_file, 'r', encoding='utf-8') as f:
    parser.readfp(f)

database_connection_param = {
       'host':parser.get(config_section, 'host').encode('utf-8'),
       'user':parser.get(config_section, 'user').encode('utf-8'),
       'pw':  parser.get(config_section, 'password').encode('utf-8'),
       'db':  parser.get(config_section, 'database').encode('utf-8')
}


#
# Open database connection
#
def sql_connect(dc):
   #print "Connecting to [%s] on [%s] as [%s]\n\n" % (dc['db'],dc['host'],dc['user'])
   try:
      db = MySQLdb.connect(dc['host'],dc['user'],dc['pw'],dc['db'])
   except MySQLdb.Error, e:
      try:
      	print "MySQL Error [%d]: %s" % (e.args[0], e.args[1])
      except IndexError:
        print "MySQL Error: %s" % str(e)	
   return(db)

#
# Clean up SQL statement to a single line
#
def exec_sql_file(cursor, sql_file):
    #print "\n[INFO] Executing SQL script file: '%s'" % (sql_file)
    statement = ""

    for line in open(sql_file):
        if re.match(r'--', line):  # ignore sql comment lines
            continue
        if not re.search(r'[^-;]+;', line):  # keep appending lines that don't end in ';'
            statement = statement + line
        else:  # when you get a line ending in ';' then exec statement and reset for next statement
            statement = statement + line
            #print "\n\n[DEBUG] Executing SQL statement:\n%s" % (statement)
            try:
                cursor.execute(statement)
            except (OperationalError, ProgrammingError) as e:
                print "\n[WARN] MySQLError during execute statement \n\tArgs: '%s'" % (str(e.args))

            statement = ""

def cleanup(value):
	if not value:
		return 'NULL'
	else:
		return str(value)

#
# read SQL and write to file block by block
#
def sql_query_block_reader(db,fname,csv,block_size=10000):
    cursor = db.cursor()
    try:
    	exec_sql_file(cursor,fname)
    except:
	print "Error: in SQL"
        db.close()
        exit(-1)
    while True:
        rows = cursor.fetchmany(block_size)
	if not rows:
		break
	for row in rows:
        	# process columns except last
  		for c in row[0:-1]:
                	csv.write(cleanup(c)+", ")
  		# now last w/o ',' and w/ '\n'
        	csv.write(cleanup(row[-1])+'\n')

def main():
    #
    # Connect to database
    #
    db = sql_connect(database_connection_param)

    try:
        #	
        # execute sql and write data to csv file
        #
	sql_query_block_reader(db,sqlfilename,csv,block_size)
    except KeyboardInterrupt:
        sys.exit(0)

if __name__ == "__main__":
    main()

