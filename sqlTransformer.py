""" 
sqlTransformer.py
 - define SqlTransformer class for bottom up query searching

 Class : Using lark.transformer, bottom-up query searching 
"""

import lark

class SqlTransformer(lark.Transformer):
    def __init__(self):
        self.sql_type = ""
        self.table_name = ""

    # Uppermost query
    def command(self, args):
        if args[0] == "exit":
            return "exit"
        else:
            return self.sql_type

    def query_list(self, args):
        return args

    def query(self, args):
        return args

    # Decide sql_type
    def create_table_query(self, args):
        self.sql_type = "CREATE TABLE"
        table_name = args[2].children[0].lower()
        print(table_name)
        return

    def drop_table_query(self, args):
        self.sql_type = "DROP TABLE"
        return

    def exlpain_table_query(self, args):
        self.sql_type = "EXPLAIN"
        return

    def describe_table_query(self, args):
        self.sql_type = "DESCRIBE"
        return

    def desc_table_query(self, args):
        self.sql_type = "DESC"
        return

    def insert_query(self, args):
        self.sql_type = "INSERT"
        return

    def delete_query(self, args):
        self.sql_type = "DELETE"
        return

    def select_query(self, args):
        self.sql_type = "SELECT"
        return

    def show_table_query(self, args):
        self.sql_type = "SHOW TABLES"
        return

    def update_table_query(self, args):
        self.sql_type = "UPDATE"
        return