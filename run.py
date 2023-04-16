import lark
import pickle
import os
from sqlTransformer import SqlTransformer
from sql_exception import *
from berkeleydb import db

"""
Module info & implementing log

PRJ 1-1 : SQL PARSER
 - CLASS : sqlTransformer -> search sql tree bottom up
 - Function :  getSqlList -> get sqls input, split into list and return it
 - Function : getParsedSql -> parsing sql, return sql tree
 - Logic : For loop, get Sql input and parsing it by sqlParser, decide its type using sqlTransformer

PRJ 1-2 : Implementing DDL & Basic DML Function
 - Refactor : Extract SqlTransformer class to sqlTransformer.py
"""

# SQL Parser using grammar.lark
with open("grammar.lark") as file:
    sqlParser = lark.Lark(file.read(), start="command", lexer="basic")

# Function : using prompt, get sql query and split into list based on ';'
def getSqlLIST():
    sentence_in = input("DB_2020-12907> ").rstrip()
    while sentence_in[-1] != ";": # Get input from pormpt while ending with ';'
        sentence_in += " " + input().rstrip()
    sentence_in = sentence_in.replace("\n", "") # remove \n
    sentence_in = sentence_in.replace("\r", "") # remove \r
    sentence_in = sentence_in.replace("\\n", "") # remove \\n
    sentence_in = sentence_in.replace("\\r", "") # remove \\r
    sentence_parsed = sentence_in.strip().split(";") # split into sql list
    sentence_parsed = sentence_parsed[:-1] # remove blank
    for i in range(len(sentence_parsed)):
        sentence_parsed[i] = sentence_parsed[i].strip() + ";" # add ';' to end of sql
    return sentence_parsed

# Function : parsing sql and return sql tree or false value (wrong sql)
def getParsedSql(sql_sentence):
    try:
        output = sqlParser.parse(sql_sentence)
    except:
        raise SyntaxError()
    else:
        return output

# Function : create table in berkeleydb
def sql_create_table(sql_data):
    table_name = sql_data["table_name"]
    columns = sql_data["columns"]
    constraints = sql_data["constraints"]

    # check CharLengthError
    for column in columns:
        if column["col_type"] == "char" and column["col_length"] < 1:
            raise CharLengthError()
        
    # check NonExistingColumnDefError
    column_set = set([col["col_name"] for col in columns])
    for constraint in constraints:
        column_name_list = constraint["column_name_list"]
        for column_name in column_name_list:
            if col_name not in column_set:
                raise NonExistingColumnDefError(col_name)

    # check TableExistenceError
    if myDB.get(pickle.dumps(table_name)):
        raise TableExistenceError()

    # check DuplicateColumnDefError
    col_names = [col['col_name'] for col in columns]
    check_duplicates = set()
    for col_name in col_names:
        if col_name in check_duplicates:
            raise DuplicateColumnDefError()
        else:
            check_duplicates.add(col_name)

    # check DuplicatePrimaryKeyDefError
    constraints_types = [cons["constraint_type"] for cons in constraints]
    check_duplicates = set()
    for constraints_type in constraints_types:
        if constraints_type == "primary" and constraints_type in check_duplicates:
            raise DuplicatePrimaryKeyDefError()
        else:
            check_duplicates.add(constraints_type)

    # check ReferenceTableExistenceError, ReferenceColumnExistenceError, ReferenceNonPrimaryKeyError, ReferenceTypeError
    for constraint in constraints:
        if constraint["constraints_type"] == "foreign":
            reference_table_name = constraint["reference_table_name"]
            reference_table_name_bin = pickle.dumps(reference_table_name)

            # check ReferenceTableExistenceError
            if not (myDB.get(reference_table_name_bin)):
                raise ReferenceTableExistenceError()
            
            reference_column_name_list = constraint["reference_column_name_list"]
            reference_table_path_bin = myDB.get(reference_table_name_bin) 
            referenceDB = db.DB()
            referenceDB.open(pickle.loads(reference_table_path_bin), dbtype=db.DB_HASH)
            reference_table_schema = pickle.loads(referenceDB.get(b'schema'))

            # check ReferenceColumnExistenceError
            reference_table_columns = reference_table_schema["columns"]
            reference_table_columns_name_list = [col["col_name"] for col in reference_table_columns]
            reference_table_columns_name_set = set(reference_table_columns_name_list)
            for reference_col_name in reference_column_name_list:
                if reference_col_name not in reference_table_columns_name_set:
                    raise ReferenceColumnExistenceError()
            
            # check ReferenceNonPrimaryKeyError
            reference_table_constraints = reference_table_schema["constraints"]
            for reference_table_constraint in reference_table_constraints:
                if reference_table_constraint["constraint_type"] == "primary":
                    reference_table_primary_column_list = reference_table_constraint["column_name_list"]
                    if len(reference_table_primary_column_list) == len(reference_column_name_list):
                        for i in range(len(reference_column_name_list)):
                            if reference_column_name_list[i] != reference_table_primary_column_list[i]:
                                raise ReferenceNonPrimaryKeyError()
                    else:
                        raise ReferenceNonPrimaryKeyError()
                    
            # check ReferenceTypeError
            column_name_list = constraint["column_name_list"]
            referenced_table_column_type_list = []
            foreign_key_column_type_list = []
            for column_name in column_name_list:
                for column in columns:
                    if column_name == column["col_name"]:
                        foreign_key_column_type_list.add((column["col_type"], column["col_length"])) 
                        break;
    
            for reference_column_name in reference_column_name_list:
                for reference_table_column in reference_table_columns:
                    if reference_column_name == reference_table_column["col_name"]:
                        referenced_table_column_type_list.add((reference_table_column["col_type"], reference_table_column["col_length"]))
                        break;
    
            for i in range(len(foreign_key_column_type_list)):
                if referenced_table_column_type_list[i] != foreign_key_column_type_list[i]:
                    raise ReferenceTypeError


    # put path/to/db into myDB
    table_name_bin = pickle.dumps(table_name)
    print("debuge1")
    db_path = './DB/{0}.db'.format(table_name)
    db_path_bin = pickle.dumps(db_path)
    print(myDB.get(table_name_bin))
    myDB.put(table_name_bin, db_path_bin)
    print(myDB.get(table_name_bin))

    # put schema into {table_name}.db
    newTableDB = db.DB()
    newTableDB.open(db_path, dbtype=db.DB_HASH, flags=db.DB_CREATE)
    sql_data_bin = pickle.dumps(sql_data)
    newTableDB.put(b'schema', sql_data_bin)
    newTableDB.close()

    print("DB_2020-12907> '{0}' table is created".format(table_name)) # if correct syntax, print sql type

    return

def sql_drop_table(sql_data):
    sql_data

def sql_explain(sql_data):
    sql_data

def sql_describe(sql_data):
    sql_data

def sql_desc(sql_data):
    sql_data

def sql_insert(sql_data):
    sql_data

def sql_delete(sql_data):
    sql_data

def sql_select(sql_data):
    sql_data

def sql_show_tables(sql_data):
    sql_data

def sql_update(sql_data):
    sql_data

def sql_runner(sql_type, sql_data):
    if sql_type == "CREATE TABLE":
        sql_create_table(sql_data)
    elif sql_type == "DROP TABLE":
        sql_drop_table(sql_data)
    elif sql_type == "EXPLAIN":
        sql_explain(sql_data)
    elif sql_type == "DESCRIBE":
        sql_describe(sql_data)
    elif sql_type == "DESC":
        sql_desc(sql_data)
    elif sql_type == "INSERT":
        sql_insert(sql_data)
    elif sql_type == "DELETE":
        sql_delete(sql_data)
    elif sql_type == "SELECT":
        sql_select(sql_data)
    elif sql_type == "SHOW TABLES":
        sql_show_tables(sql_data)
    elif sql_type == "UPDATE":
        sql_update(sql_data)

# Make sqlTransformer instance and program repeating flag
sqlTF = SqlTransformer()
flag = True

# Open and create mydb
myDB = db.DB()
file_path = "./DB/myDB.db"
if os.path.exists(file_path):
    myDB.open(file_path, dbtype=db.DB_HASH)
else:
    myDB.open('./DB/myDB.db', dbtype=db.DB_HASH, flags=db.DB_CREATE)

# Main program : parsing sql until get 'exit;'
while flag:
    parsing_list = getSqlLIST()
    for sql_sentence in parsing_list:
        try:
            parsed_output = getParsedSql(sql_sentence) # get sql tree
            # print(parsed_output) #test
            # print(parsed_output.pretty()) #test
            sql_type, sql_data = sqlTF.transform(parsed_output) # get sql type
            # print(sql_data)
            if sql_type == "exit": # if 'exit;' then break program
                flag = False
                myDB.close()
                break
            sql_runner(sql_type, sql_data)
            print("DB_2020-12907> '{0}' requested".format(sql_type)) # if correct syntax, print sql type
        except (SyntaxError, 
                DuplicateColumnDefError, 
                DuplicatePrimaryKeyDefError, 
                ReferenceTypeError, 
                ReferenceNonPrimaryKeyError,
                NonExistingColumnDefError,
                TableExistenceError,
                CharLengthError,
                NoSuchTable,
                DropReferencedTableError,
                SelectTableExistenceError
                ) as e:
            print(e) # if error occurred, print error message
            break