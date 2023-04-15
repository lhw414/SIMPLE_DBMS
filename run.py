import lark
from sqlTransformer import SqlTransformer
from sql_exception import *
# from berkeleydb import db

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

# Function List : run sql using berkeleydb
def sql_create_table(sql_data):
    table_name = sql_data["table_name"]
    

def sql_drop_table(sql_data):
    sql_data

def sql_explain(sql_data):
    sql_data

def sql_describe(sql_data):
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

# Make sqlTransformer instance and program repeating flag
sqlTF = SqlTransformer()
flag = True

# Open mydb
# myDB = db.DB()
# myDB.open('myDB.db', dbtype=db.DB_HASH, flags=db.DB_CREATE)

# Main program : parsing sql until get 'exit;'
while flag:
    parsing_list = getSqlLIST()
    for sql_sentence in parsing_list:
        try:
            parsed_output = getParsedSql(sql_sentence) # get sql tree
            print(parsed_output) #test
            print(parsed_output.pretty()) #test
            sql_type, sql_data = sqlTF.transform(parsed_output) # get sql type

            if sql_type == "exit": # if 'exit;' then break program
                flag = False
                # myDB.close()
                break
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