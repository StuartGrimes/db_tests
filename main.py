from database.mysqldb import MySQLDatabase

myConnection = MySQLDatabase('employees',
                  'Stuart',
                  'Ducati1099'
                  )

kwargs = {'where': 'emp_no < 10010', 'group': 'emp_no', 'order':'emp_no', 'orderdir': 'DESC', 'limit': 5}

results = myConnection.select('salaries', columns=['emp_no'], aggregates={'max': 'salary'}, named_tuples=False, **kwargs)
#
# print results

for records in results:
    print records

# tables_list = myConnection.get_available_tables()
# print tables_list
#
# columns_list = myConnection.get_columns_for_table('employees')
# print columns_list
#
# print myConnection.database_name
#
# for table in tables_list:
#     print "\nTable Name : %s" % table
#     print "Column Names : ",
#     for column in myConnection.get_columns_for_table(table):
#         print "%s |" % column[0],

