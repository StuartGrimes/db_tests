import MySQLdb

from collections import namedtuple


class MySQLDatabase:
    def __init__(self, database_name, username, password, host='localhost'):
        try:
            self.db = MySQLdb.connect(db=database_name, host=host, user=username, passwd=password)

            self.database_name = database_name

            print 'Connected to MySql!'
        except MySQLdb.Error, e:
            print e

    def __del__(self):
        if hasattr(self, 'db'):  # close our connection to free it up in the pool
            self.db.close()
            print 'MySQL Connection Closed'

    def get_available_tables(self):
        cursor = self.db.cursor()
        cursor.execute('SHOW TABLES;')
        self.tables = cursor.fetchall()
        cursor.close()
        return self.tables

    def get_columns_for_table(self, table_name):
        cursor = self.db.cursor()
        cursor.execute("SHOW COLUMNS FROM %s" % table_name)

        self.columns = cursor.fetchall()

        cursor.close()

        return self.columns

    def convert_to_named_tuples(self, cursor):
        results = None

        names = " ".join(d[0] for d in cursor.description)  # gets the column names (cursor has a description property)
        klass = namedtuple("Results", names)

        try:
            results = map(klass._make, cursor.fetchall())
        except MySQLdb.ProgrammingError:
            pass
        return results

    def select(self, table, columns=None, aggregates=None, named_tuples=False, **kwargs):
        sql_str = "SELECT "

        # add colums or just the wildcard
        if not columns:
            sql_str += "* "
        else:
            for column in columns:
                sql_str += "%s, " % column
        #add aggregates if present
        if aggregates:
            for aggregate in aggregates:
                sql_str += "%s(%s), " % (aggregate, aggregates[aggregate])

            sql_str = sql_str[:-2]  # remove last comma space
        # add the table to select from
        sql_str += " FROM %s.%s" % (self.database_name, table)

        # there is a join clause attached
        if kwargs.has_key('join'):
            sql_str += " JOIN %s" % kwargs.get('join')

        # there is a where clause attahced.
        if kwargs.has_key('where'):
            sql_str += " WHERE %s" % kwargs.get('where')

        # there is a group clause attahced.
        if kwargs.has_key('group'):
            sql_str += " GROUP BY %s" % kwargs.get('group')

        # there is an order by clause attached.
        if kwargs.has_key('order'):
            sql_str += " ORDER BY %s %s" % (kwargs.get('order'), kwargs.get('orderdir'))

        # there is a limit clause attahced.
        if kwargs.has_key('limit'):
            sql_str += " LIMIT %s" % kwargs.get('limit')

        sql_str += ";"

        # return sql_str

        cursor = self.db.cursor()

        cursor.execute(sql_str)

        if named_tuples:
            results = self.convert_to_named_tuples(cursor)
        else:
            results = cursor.fetchall()

        cursor.close()

        return results
