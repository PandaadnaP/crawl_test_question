import pymssql, _mssql
import pymysql

class SQL(object):
    server = None
    user = None
    password = None
    database = None
    connect = None
    connect_mssql = None
    connect_mysql = None
    cursor = None

    def __init__(self, para_server, para_user, para_password, para_database):
        self.server = para_server
        self.user = para_user
        self.password = para_password
        self.database = para_database
        self.connect = None
        self.cursor = None

    def connect_sql(self):
        """
        no parameter
        :return: no return
        """
        self.connect = pymssql.connect(self.server, self.user, self.password, self.database)
        self.connect_mssql = _mssql.connect(server=self.server, user=self.user, password=self.password, database=self.database)
        self.cursor = self.connect.cursor()
        self.connect_mysql = pymysql.connect(server=self.server, user=self.user, password=self.password, database=self.database)

    def create_table(self, comd):
        """
        execute command and return a list of information
        :param cursor: cursor of the database
        :param comd: the command that needed to be execute
        :return: 1 for sucess , 0 for fail
        """
        try:
            self.connect_mssql.execute_non_query(comd)
            return 1
        except _mssql.MssqlDatabaseException as e:
            if e.number == 2714:
                print('table exist!')
            else:
                print('error')
            return 0

    def delete_tables(self, table_name):
        """
        delete table
        :param table_name: name of table needed to be delete
        :return: 1 for sucess , 0 for fail
        """
        try:
            self.connect_mssql.execute_non_query(
                """
                DROP TABLE %s;
                """%(table_name)
            )
            return 1
        except _mssql.MssqlDatabaseException as e:
            if e.number == 1051 or e.number == 1046 or e.number == 3701:
                print("table not exist!")
                return 0
            else:
                print("error!")
                return 0

    def insert_info(self, table_name, dic_of_insert_info):
        str_col = ""
        str_val = ""
        flag = 0
        for column_name in dic_of_insert_info:
            value = dic_of_insert_info[column_name]
            if flag == 1:
                str_col += ','
                str_val += ','
            str_col += "%s"%(column_name)
            str_val += self._str_attr(value)
            flag = 1

        command ="INSERT INTO %s " % (table_name) + '(' + str_col + ')' + " VALUES " + '(' + str_val + ')'
        print(command)
        try:
            self.connect_mssql.execute_non_query(command)
            return 1
        except _mssql.MssqlDatabaseException as e:
            print('fail inserting info, ' + str(e.number))
            return 0

    def update_info(self, table_name, dic_of_primary_key, dic_of_update_info):
        str_primary_key = ""
        flag = 0
        for item in dic_of_primary_key:
            key = dic_of_primary_key[item]
            if flag == 1:
                str_primary_key += ' AND '
            str_primary_key += (item + '=' + self._str_attr(key))
            flag = 1
        sucess = 0
        for column_name in dic_of_update_info:
            value = dic_of_update_info[column_name]
            command = "UPDATE %s SET "%(table_name) + column_name + '=' + self._str_attr(value) + ' WHERE ' + str_primary_key
            print(command)
            try:
                self.connect_mssql.execute_non_query(command)
                sucess = 1

            except _mssql.MssqlDatabaseException as e:
                print('fail updating info, ' + str(e.number))
                sucess = 0

            if sucess == 1:
                continue
            else:
                return 0
        return 1
    def delete_info(self, table_name, dic_of_primary_key):
        str_primary_key = ""
        str_update_info = ""
        flag = 0
        for item in dic_of_primary_key:
            key = dic_of_primary_key[item]
            if flag == 1:
                str_primary_key += ' AND '
            str_primary_key += (item + '=' + self._str_attr(key))
            flag = 1
        command = "DELETE FROM %s WHERE %s"%(table_name, str_primary_key)
        print(command)
        try:
            self.connect_mssql.execute_non_query(command)
            return 1
        except _mssql.MssqlDatabaseException as e:
            print('fail deleting info, ' + str(e.number))
            return 0

    def add_column(self, table_name, column_name, column_type, null_or_not):
        """
        add a colum
        :param table_name: table name (str)
        :param coloun_name: column name (str)
        :param coloun_type: type of column (str) , like 'char(10)'
        :param null_or_not: true for not null, false for can be null (boolean)
        :return:
        """
        if null_or_not == True:
            command = "ALTER TABLE %s ADD %s %s NOT NULL"%(table_name, column_name, column_type)
        else:
            command = "ALTER TABLE %s ADD %s %s"%(table_name, column_name, column_type)
        print(command)
        try:
            self.connect_mssql.execute_non_query(command)
            return 1
        except _mssql.MssqlDatabaseException as e:
            print('fail adding column, ' + str(e.number))
            return 0


    def delete_column(self, table_name, column_name):
        """
        add a colum
        :param table_name: table name (str)
        :param coloun_name: column name (str)
        :param coloun_type: type of column (str) , like 'char(10)'
        :param null_or_not: true for not null, false for can be null (boolean)
        :return:
        """
        command = "ALTER TABLE %s DROP COLUMN %s"%(table_name, column_name)
        print(command)
        try:
            self.connect_mssql.execute_non_query(command)
            return 1
        except _mssql.MssqlDatabaseException as e:
            print('fail deleting column, ' + str(e.number))
            return 0

    def select_info(self, table_name, column_name, dic_of_primary_key):
        str_primary_key = ""
        flag = 0
        returnlist = []
        for item in dic_of_primary_key:
            key = dic_of_primary_key[item]
            if flag == 1:
                str_primary_key += ' AND '
            str_primary_key += (item + '=' + self._str_attr(key))
            flag = 1

        command = "SELECT "
        flag = 0
        for column in column_name:
            if flag == 0:
                command += column
                flag = 1
            else:
                command += (',' + column)

        command += ' '


        if len(dic_of_primary_key) == 0:
            command += "FROM %s"%(table_name)
        else:
            command += "FROM %s WHERE %s"%(table_name, str_primary_key)

        print(command)

        self.cursor.execute(command)
        row = self.cursor.fetchone()
        if row == None:
            print('nothing')
        while row:
            returnlist.append(row)
            row = self.cursor.fetchone()
        return returnlist


    def execute(self):
        pass

    def add_primary_key(self,table_name, list_of_primary_key):
        str_prim = ''
        flag = 0
        for i in list_of_primary_key:
            if flag == 0:
                str_prim += i
                flag = 1
            else:
                str_prim += (',' + i)

        command = 'ALTER TABLE %s ADD PRIMARY KEY (%s)' % (table_name, str_prim)
        print(command)
        try:
            self.connect_mssql.execute_non_query(command)
            return 1
        except _mssql.MssqlDatabaseException as e:
            print('fail adding primary key, ' + str(e.number))
            return 0


    def _str_attr(self, added_str):
        if type(added_str) == str:
            return "'%s'"%(added_str)
        elif type(added_str) == int:
            return "%d"%(added_str)
        else:
            return "'%f'"%(added_str)

    def close(self):
        """
        close the connection
        :return: None
        """
        self.connect.close()
        self.connect_mssql.close()

