import mysql.connector
import time

db = mysql.connector.connect(
    host = '172.18.1.135',
    #port = 3306,
    user = 'user001',
    passwd = '123456',
    database = 'apollo_bag_data'
)

cur = db.cursor()

# select = ("select {} from {};")


# cur.execute(select.format('*','test'))

# query = ("select table_name from information_schema.tables \
#             where table_schema = 'apollo_bag_data';")
# cur.execute(query)


# table_list = list(cur)

# print ('rosbags' in [i[0] for i in table_list])

# print ("abc: 'd' ")

# def table_exists(database, table_name):
#         check_query = ("SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES\
#                         WHERE TABLE_SCHEMA = '{}'".format(database))
#         cur.execute(check_query)
#         table_list = list(cur)
#         print (table_list)
#         return table_name in [i[0] for i in table_list]

# result = table_exists('apollo_bag_data','rosbags')
# print (result)


# def insert_into(table_name,data): #data has to be tuple
#         insert_query = ("INSERT INTO {} VALUES {};".format(table_name,data))
#         print (insert_query)
#         cur.execute(insert_query)

# def insert_bag(bag_date,bag_cartype,bag_description):
#     bag_id = time.time()
#     bag_info = bag_id,bag_date,bag_cartype,bag_description
#     insert_into('rosbags', bag_info)

# insert_bag(
#     bag_date = '2018-11-30 14:30:00',
#     bag_cartype = 'test_veh',
#     bag_description = 'This is a test entry'
# )

print ('get here')
query = "SELECT * FROM rosbags;"
cur.execute(query)
print ('get here')
id_ = []
date = []
type_ = []
text_ = []
tt = ['listed','data',2]
query = 'insert into table_name value %r'%(tuple(tt),)
for a,b,c,d in cur:
    id_.append(a)
    date.append(b)
    type_.append(c)
    text_.append(d)


print (query)
print (id_)
print (date)

print ('get here')

# for id, date, car,des in cur:
#     print(id, date, car,des)

# table_name = "rosbags"
# check_query = ("SELECT * FROM INFORMATION_SCHEMA.TABLES\
#                         WHERE TABLE_NAME = {}".format(table_name))
# cur.execute(check_query)

# for i in cur:
#     print (cur)

# dd = {str:'VARCHAR(255)',float:'FLOAT(64,4)',int:'INT'}

# def create_table(table_name,columns,datatype): #columns and datatype are lists
#         query = ("CREATE TABLE {} ( \n".format(table_name))
#         query_seg = ("")
#         for i, col_name in enumerate(columns[:-1]):
#             query_seg = ('{} {},\n'.format(col_name,dd[datatype[i]]))
#             query += query_seg
#         query_seg = ('{} {});'.format(col_name[-1],dd[datatype[-1]]))
#         #query_seg = ('PRIMARY KEY ({}));'.format(key))
#         query += query_seg
#         print query
#         #cursor.execute(query)



cur.close()
db.close()
