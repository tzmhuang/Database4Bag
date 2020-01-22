# def foo (arg1, arg2, arg3):
#     print (arg1,arg2,arg3)

# foo(
#     arg2 = 'second',
#     arg3 = 'third',
#     arg1 = ['name1','name2']
# )

# string1 = ('{} part'.format('first'))
# string2 = 'second part'
# print (string1+string2) 

def create_table(name,columns,datatype,key): #columns is a list containing column names
        dd = {'str':'varchar(255)','float':'FLOAT(63,4)'}
        query = ("CREATE TABLE {} ( \n".format(name))
        query_seg = ("")
        for i, col_name in enumerate(columns):
            query_seg = ('{} {} NOT NULL,\n'.format(col_name,dd[datatype[i]]))
            query += query_seg
        
        query_seg = ('PRIMARY KEY ({}));'.format(key))
        query += query_seg
        print (query)

create_table(
    name = 'table_1',
    columns = ['first_col','second_col'],
    datatype = ['str','float'],
    key = 'first_col'
)