""" 
sqlTransformer.py
 - define SqlTransformer class for bottom up query searching

 Class : Using lark.transformer, bottom-up query searching 
"""

import lark
from sql_exception import *

class SqlTransformer(lark.Transformer):
    def __init__(self):
        self.sql_type = ""
        self.sql_data = {}

    # Uppermost query
    def command(self, args):
        if args[0] == "exit":
            return "exit", self.sql_data
        else:
            return self.sql_type, self.sql_data

    def query_list(self, args):
        return args

    def query(self, args):
        return args

    # Decide sql_type
    def create_table_query(self, args):
        self.sql_type = "CREATE TABLE"
        # find table name
        table_name = args[2].children[0].lower()
        self.sql_data["table_name"] = table_name
        column_definition_iter = args[3].find_data("column_definition")
        # find columns constraints
        columns = []
        for it in column_definition_iter:
            # find col_name
            col_name = it.children[0].children[0].lower()
            # find col_type
            col_type = it.children[1].children[0].lower()
            # find col_length
            col_length = None
            if col_type == "char":
                col_length = int(it.children[1].children[2])
                if col_length < 1:
                    raise CharLengthError() # check CharLengthError
            # check col is not null
            col_not_null = False
            if len(it.children) > 2:
                col_not_null = True
            # append columns data
            columns.append({
                "col_name": col_name,
                "col_type": col_type,
                "col_length": col_length,
                "col_not_null": col_not_null,
            })   
        self.sql_data["columns"] = columns

        # find table constraints
        table_constraint_definition = args[3].find_data("table_constraint_definition")
        constraints = []
        primary_key_duplicate = False
        for it in table_constraint_definition:
            # find constraint type
            constraint_type = it.children[0].children[0].lower()
            # check DuplicatePrimaryKeyDefError
            if constraint_type == True and constraint_type == "primary":
                raise DuplicatePrimaryKeyDefError()
            if constraint_type == "primary":
                primary_key_duplicate = True
                # find primary column name list
                column_name_tree = it.children[0].children[2]
                column_name_list = []
                for child in column_name_tree.children:
                    if child == "(" or child == ")":
                        continue
                    column_name_list.append(child.children[0].value.lower())
                # append primary constraints
                constraints.append({
                    "constraint_type" : constraint_type,
                    "column_name_list" : column_name_list,
                    "reference_table_name" : None,
                    "reference_column_name_list" : None,
                })
            elif constraint_type == "foreign":
                # find foreign column name list
                column_name_tree = it.children[0].children[2]
                column_name_list = []
                for child in column_name_tree.children:
                    if child == "(" or child == ")":
                        continue
                    column_name_list.append(child.children[0].value.lower())
                # find reference table name
                reference_table_name = it.children[0].children[4].children[0].value.lower()
                # find reference column name list
                reference_column_name_tree = it.children[0].children[5]
                reference_column_name_list = []
                for child in reference_column_name_tree.children:
                    if child == "(" or child == ")":
                        continue
                    reference_column_name_list.append(child.children[0].value.lower())
                # append foreign constraints
                constraints.append({
                    "constraint_type" : constraint_type,
                    "column_name_list" : column_name_list,
                    "reference_table_name" : reference_table_name,
                    "reference_column_name_list" : reference_column_name_list,
                })                               
        self.sql_data["constraints"] = constraints

        return

    def drop_table_query(self, args):
        self.sql_type = "DROP TABLE"
        #find table name
        table_name = args[2].children[0].lower()
        self.sql_data["table_name"] = table_name

        return

    def exlpain_table_query(self, args):
        self.sql_type = "EXPLAIN"
        #find table name
        table_name = args[1].children[0].lower()
        self.sql_data["table_name"] = table_name

        return

    def describe_table_query(self, args):
        self.sql_type = "DESCRIBE"
        #find table name
        table_name = args[1].children[0].lower()
        self.sql_data["table_name"] = table_name

        return

    def desc_table_query(self, args):
        self.sql_type = "DESC"
        #find table name
        table_name = args[1].children[0].lower()
        self.sql_data["table_name"] = table_name

        return

    def insert_query(self, args):
        self.sql_type = "INSERT"
        # find table name
        table_name = args[2].children[0].lower()
        self.sql_data["table_name"] = table_name
        # find col name list
        col_name_tree = args[3]
        col_name_list = None
        if col_name_tree is not None:
            col_name_list = []
            for child in col_name_tree.children:
                    if child == "(" or child == ")":
                        continue
                    col_name_list.append(child.children[0].value.lower())
        self.sql_data["col_name_list"] = col_name_list
        # find value list
        value_tree = args[5]
        value_list = []
        for child in value_tree.children:
            if child == "(" or child == ")":
                continue
            value_list.append(child.children[0].value.lower())
        self.sql_data["value_list"] = value_list

        return

    def delete_query(self, args):
        self.sql_type = "DELETE"
        return

    def select_query(self, args):
        self.sql_type = "SELECT"
        # find table name
        table_name = args[2].children[0].children[1].children[0].children[0].children[0].lower()
        self.sql_data["table_name"] = table_name
        
        return

    def show_table_query(self, args):
        self.sql_type = "SHOW TABLES"
        self.sql_type = {}
        return

    def update_table_query(self, args):
        self.sql_type = "UPDATE"
        return