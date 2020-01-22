import psycopg2

print ('got here')

conn = psycopg2.connect(dbname = "postgres",
                user = "user001",
                password = "123456",
                host = "172.18.0.231")
class sql_connector():
    
    def __init__(self,user,password,host,database):
        self.username = user
        self.password = password
        self.host = host
        self.database = database
        self.connection = mysql.connector.connect(
            user = self.username,
            password = self.password,
            host = self.host,
            dbname = self.database
        )
        self.cursor = self.connection.cursor()
        self.dd = {str:'VARCHAR(255)',float:'FLOAT(64,4)',int:'INT','bag_id':'VARCHAR(32)'} # Datatypes allowed

    def disconnect(self):
        self.connection.close()

    def commit(self):
        self.connection.commit() 

    def create_table(self,table_name,columns,datatype): #columns and datatype are lists
        query = ("CREATE TABLE {} ( \n".format(table_name))
        for i, col_name in enumerate(columns[:-1]):
            query_seg = ('{} {},\n'.format(col_name,self.dd[datatype[i]]))
            query += query_seg
        query_seg = ('{} {});'.format(columns[-1],self.dd[datatype[-1]]))
        query += query_seg
        self.cursor.execute(query)
    
    def delete_table(self,table_name):
        del_query = ("DROP TABLE {}".format(table_name))
        self.cursor.execute(del_query)
    
    def insert_into(self,table_name,data): #data has to be tuple
        insert_query = ("INSERT INTO {} VALUES {};".format(table_name,data))
        self.cursor.execute(insert_query)
    
    def table_exists(self, table_name):
        check_query = ("SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES\
                        WHERE TABLE_SCHEMA = '{}'".format(self.database))
        self.cursor.execute(check_query)
        table_list = list(self.cursor)
        return table_name in [i[0] for i in table_list] 


class Bag2db(object):
    def __init__(self,bag_file,connector):
        self.bag = rosbag.Bag(bag_file)
        self.connector = connector
        self.types = list(self.bag.get_type_and_topic_info()[0])
        self.topics = list(self.bag.get_type_and_topic_info()[1])

    def bag_info(self, bag_date, bag_vehtype, bag_description):    
        self.bag_id = uuid.uuid4().hex
        self.bag_info = self.bag_id,bag_date,bag_vehtype,bag_description
        self.connector.insert_into('rosbags',self.bag_info)
    
    def localization2db(self):
        self.time = 0
        table_name = 'apollo_localization_pose' ######
        localization_msg = self.bag.read_messages(topics = ['/apollo/localization/pose'])
        self.__column_list = ['bag_id']
        self.__data_type = ['bag_id']
        if not self.connector.table_exists(table_name):
            print 'inside if'
            init_msg = list(localization_msg)[0]
            self.__fetch_column(init_msg,'message','')
            print self.__column_list
            print self.__data_type
            self.connector.create_table(table_name,self.__column_list,self.__data_type)
            print 'Table created'
        for msg in localization_msg:
            self.__data_list = [self.bag_id]
            self.__fetch_data(msg,'message')
            data = tuple(self.__data_list)
            self.connector.insert_into(table_name,data)

    
    def control2db(self):
        table_name = 'apollo_control' ######
        control_msg = self.bag.read_message(topics = ['/apollo/control'])
        self.__column_list = []
        self.__data_type = []
        if not self.connector.table_exists(table_name):
            init_msg = list(control_msg)[0]
            self.__fetch_column(init_msg,'message','')
            self.connector.create_table(table_name,self.__column_list,self.__data_type)
        for msg in control_msg:
            self.__data_list = [self.bag_id]
            self.__fetch_data(msg,'message')
            data = tuple(self.__data_list)
            self.connector.insert_into(table_name,data)
    
    def __fetch_column(self,msg,field_name,prev_field_name):
        msg = getattr(msg,field_name)
        field_name = prev_field_name+'_'+field_name
        if ('DESCRIPTOR' not in dir(msg)):  # not a message
            if msg and msg == msg and type(msg) in self.connector.dd:          # check if msg is nonempty and non-nan, if msg != msg, msg is nan. 
                self.__column_list.append(field_name)
                self.__data_type.append(type(msg))
        else:
            names = msg.DESCRIPTOR.fields_by_name.keys()
            for name in names:
                self.__fetch_column(msg,name,field_name)

    def __fetch_data(self,msg,field_name):
        msg = getattr(msg,field_name)
        if ('DESCRIPTOR' not in dir(msg)): # not a message
            if msg and msg == msg and type(msg) in self.connector.dd:
                self.__data_list.append(msg)
        else:
            names = msg.DESCRIPTOR.fields_by_name.keys()
            for name in names:
                self.__fetch_data(msg,name)

    def close(self):
        self.bag.close()
        self.connector.commit()
        self.connector.disconnect()



##create table

def main():
    ## Connect database
    connector = sql_connector(
                user = 'user001',
                password = '123456',
                host = '172.18.1.135',
                database = 'apollo_bag_data'  
                )
    bag_file = '/apollo/bags/2018-11-27-11-09-11_0.bag'
    sender = Bag2db(bag_file,connector)
    # datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    
    sender.bag_info(
                bag_date = '2018-12-06 00:00:00',    # str: "YYYY-MM-DD hh:mm:ss"
                bag_vehtype = 'bus', # str: 'the type of the vehicle'
                bag_description = 'This is a test entry' #str: 'Further descriptions of the bag'
                )
    query = ('SELECT * FROM rosbags;')
    connector.cursor.execute(query)
    for a,b,c,d in connector.cursor:
        print a,b,c,d,'#'
    ## Fetching data and inserting data
    #sender.localization2db()
    #query = ('SELECT * FROM apollo_localization_pose;')
    #sender.connector.cursor.execute(query)
    # for b in sender.connector.cursor:
    #     print b
    #senfer.control2db()
    #sender.close()


if __name__ == '__main__':
    main()