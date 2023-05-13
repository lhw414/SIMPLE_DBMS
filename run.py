import lark
import pickle
import os
import datetime
import itertools
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
 - CLASS : sql error class added
 - Function : sql query runner function added
 - Function : sql function added : sql_create_table, sql_drop_table, sql_explain(=sql_desc, sql_describe), sql_insert, sql_select, sql_show_tables
 - Logic : For loop, get sql input and parsing it by sqlParser, get sql data and type using sqlTransformer
           and using sql_runner function, run sql_[sql_type] function.

PRJ 1-3 : Implementing DML Function
 - 

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

    # check do not create 'mydb'
    if table_name == "mydb":
        raise mydbCreateError()

    # check CharLengthError
    for column in columns:
        if column["col_type"] == "char" and column["col_length"] < 1:
            raise CharLengthError()
        
    # check NonExistingColumnDefError
    column_set = set([col["col_name"] for col in columns])
    for constraint in constraints:
        column_name_list = constraint["column_name_list"]
        for column_name in column_name_list:
            if column_name not in column_set:
                raise NonExistingColumnDefError(column_name)

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
        if constraint["constraint_type"] == "foreign":
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
                        foreign_key_column_type_list.append((column["col_type"], column["col_length"])) 
                        break;
    
            for reference_column_name in reference_column_name_list:
                for reference_table_column in reference_table_columns:
                    if reference_column_name == reference_table_column["col_name"]:
                        referenced_table_column_type_list.append((reference_table_column["col_type"], reference_table_column["col_length"]))
                        break;
    
            for i in range(len(foreign_key_column_type_list)):
                if referenced_table_column_type_list[i] != foreign_key_column_type_list[i]:
                    raise ReferenceTypeError


    # put path/to/db into myDB
    table_name_bin = pickle.dumps(table_name)
    db_path = './DB/{0}.db'.format(table_name)
    db_path_bin = pickle.dumps(db_path)
    myDB.put(table_name_bin, db_path_bin)

    # put schema into {table_name}.db
    newTableDB = db.DB()
    newTableDB.open(db_path, dbtype=db.DB_HASH, flags=db.DB_CREATE)
    sql_data_bin = pickle.dumps(sql_data)
    newTableDB.put(b'schema', sql_data_bin)
    newTableDB.close()

    print("DB_2020-12907> '{0}' table is created".format(table_name)) # if correct syntax, print sql type

    return

# Function : drop table in berkeleydb
def sql_drop_table(sql_data):
    table_name = sql_data["table_name"]
    table_name_bin = pickle.dumps(table_name)
    # check NoSuchTable Error
    if not (myDB.get(table_name_bin)):
        raise NoSuchTable()    
    # check referenced other table
    cursor = myDB.cursor()
    while x := cursor.next():
        referenced_table, referenced_table_path = pickle.loads(x[0]), pickle.loads(x[1])
        if referenced_table != table_name:
            referencedDB = db.DB()
            referencedDB.open(referenced_table_path, dbtype=db.DB_HASH)
            referenced_table_schema = pickle.loads(referencedDB.get(b'schema'))
            # check DropReferencedTableError
            for constraint in referenced_table_schema["constraints"]:
                if constraint["constraint_type"] == "foreign":
                    if constraint["reference_table_name"] == table_name:
                        raise DropReferencedTableError(table_name)
                    
    # delete table
    table_path = pickle.loads(myDB.get(table_name_bin))
    myDB.delete(table_name_bin)
    os.remove(table_path)

    print("DB_2020-12907> '{0}' table is dropped".format(table_name))

# Function : explain table in berkeleydb
def sql_explain(sql_data):
    table_name = sql_data["table_name"]
    table_name_bin = pickle.dumps(table_name)
    # check NoSuchTable Error
    if not (myDB.get(table_name_bin)):
        raise NoSuchTable()
    # get table schema
    table_path = pickle.loads(myDB.get(table_name_bin))
    tableDB = db.DB()
    tableDB.open(table_path, dbtype=db.DB_HASH)
    table_schema = pickle.loads(tableDB.get(b'schema'))
    # print table schema
    column_headers = ['column_name', 'type', 'null', 'key']
    print('-' * 65)
    print(f"table_name [{table_schema['table_name']}] ")
    print(
        f"{column_headers[0]:20} {column_headers[1]:20} {column_headers[2]:20} {column_headers[3]:20}")
    for col in table_schema['columns']:
        null_val = 'N' if col['col_not_null'] else 'Y'

        if col['col_type'] == "char":
            col_type = f"char({col['col_length']})"
        else:
            col_type = col['col_type']
        key_type = ""
        for constraint in table_schema["constraints"]:
            if constraint["constraint_type"] == "primary":
                if col["col_name"] in constraint["column_name_list"]:
                    key_type = "PRI"
                    break
            elif constraint["constraint_type"] == "foreign":
                if col["col_name"] in constraint["column_name_list"]:
                    key_type = "FOR"
                    break
        print(f"{col['col_name']:20} {col_type:20} {null_val:20} {key_type:20}")
    print('-' * 65)

# Function : insert data into table in berkeleydb
def sql_insert(sql_data): # todo : implement
    timestamp = hash(str(datetime.datetime.now().timestamp())[:100])
    table_name = sql_data["table_name"]
    table_name_bin = pickle.dumps(table_name)
    # check NoSuchTable Error
    if not (myDB.get(table_name_bin)):
        raise NoSuchTable()
    # get insert (column_name, value) tuple
    insert_data = []
    insert_array = []
    if sql_data["col_name_list"] is not None:
        for i in range(len(sql_data["value_list"])):
            insert_data.append((sql_data["col_name_list"][i], sql_data["value_list"][i]))
    else:
        insert_array = sql_data["value_list"][:]
    # get table schema
    table_path = pickle.loads(myDB.get(table_name_bin))
    tableDB = db.DB()
    tableDB.open(table_path, dbtype=db.DB_HASH)
    table_schema = pickle.loads(tableDB.get(b'schema'))
    table_columns = table_schema["columns"]
    # check InsertColumnExistenceError
    if sql_data["col_name_list"] is not None:
        for insert_tuple in insert_data:
            columnExistence = False
            for table_column in table_columns:
                if table_column["col_name"] == insert_tuple[0]:
                    columnExistence = True
            if not columnExistence:
                raise InsertColumnExistenceError(insert_tuple[0])
    # check InsertTypeMismatchError(# attributes)
    if len(insert_array) != len(table_columns) and len(insert_data) != len(table_columns):
        raise InsertTypeMismatchError()
    # insert data to insert array
    if sql_data["col_name_list"] is not None:
        for table_column in table_columns:
            for insert_tuple in insert_data:
                if table_column["col_name"] == insert_tuple[0]:
                    insert_array.append(insert_tuple[1])
    # check InsertTypeMismatchError(type of attributes), InsertColumnNonNullableError
    for idx, insert_value in enumerate(insert_array):
        attributeType = table_columns[idx]["col_type"]
        attributeNotAcceptNull = table_columns[idx]["col_not_null"]
        if (insert_value == "null"):
            if attributeNotAcceptNull:
                raise InsertColumnNonNullableError(table_columns[idx]["col_name"])
            continue
        if attributeType == "int":
            try:
                int(insert_value)
            except ValueError:
                raise InsertTypeMismatchError()
        elif attributeType == "date":
            try:
                datetime.datetime.strptime(insert_value, '%Y-%m-%d')
            except ValueError:
                raise InsertTypeMismatchError()
        else: # truncate
            if "'" not in insert_value or '"' not in insert_value:
                raise InsertTypeMismatchError()
            charValue = insert_value[1:-1]
            if len(charValue) > table_columns[idx]["col_length"]:
                charValue = insert_value[:table_columns[idx]["col_length"]]
            insert_array[idx] = charValue

    # insert data
    tableDB.put(pickle.dumps(timestamp), pickle.dumps(insert_array))

    print("DB_2020-12907> The row is inserted")

# Function : delete data in table in berkeleydb
def sql_delete(sql_data): # todo : implement
    table_name = sql_data["table_name"]
    where_clause = sql_data["where_clause"]
    table_name_bin = pickle.dumps(table_name)
    # check NoSuchTable Error
    if not (myDB.get(table_name_bin)):
        raise NoSuchTable()
    # get all row of tables
    table_path = pickle.loads(myDB.get(table_name_bin))
    tableDB = db.DB()
    tableDB.open(table_path, dbtype=db.DB_HASH)
    table_schema = pickle.loads(tableDB.get(b'schema'))
    cursor = tableDB.cursor()
    row_list = []
    delete_list = []
    while x:= cursor.next():
        row_datetime, row_data = x[0], pickle.loads(x[1])
        if row_datetime != b"schema":
            row_list.append((row_datetime, row_data))
    # check where clause
    if where_clause is not None:
        # check error
        if len(row_list) == 0:
            replace_with_true_for_error_checking(where_clause, table_name, table_schema)
        # check each row
        for row_tuple in row_list:
            boolean_stack = replace_with_true(where_clause, table_name, table_schema, row_tuple)
            if evaluate_boolean_stack(boolean_stack):
                delete_list.append(row_tuple)
        delete_rows_num = len(delete_list)
        for delete_row_tuple in delete_list:
            tableDB.delete(delete_row_tuple[0])
    else:
        delete_rows_num = len(row_list)
        for row in row_list:
            tableDB.delete(row[0])

    print("DB_2020-12907> {0} row(s) are deleted".format(delete_rows_num))

def replace_with_true_for_error_checking(expression, table_name, table_schema):
    stack = []
    for item in expression:
        if isinstance(item, list):
            # 리스트인 경우 재귀적으로 탐색하여 결과를 스택에 추가
            stack.append(replace_with_true(item, table_name, table_schema))
        elif isinstance(item, dict):
            # 딕셔너리인 경우 `True` 값을 스택에 추가
            stack.append(check_where_clause_error(item["predicate"], table_name, table_schema))
        else:
            # 다른 타입의 항목인 경우 그대로 스택에 추가
            stack.append(item.value)
    return stack

def replace_with_true(expression, table_name, table_schema, row_tuple):
    stack = []
    for item in expression:
        if isinstance(item, list):
            # 리스트인 경우 재귀적으로 탐색하여 결과를 스택에 추가
            stack.append(replace_with_true(item, table_name, table_schema, row_tuple))
        elif isinstance(item, dict):
            # 딕셔너리인 경우 `True` 값을 스택에 추가
            stack.append(evaluate_conditions(item["predicate"], table_name, table_schema, row_tuple))
        else:
            # 다른 타입의 항목인 경우 그대로 스택에 추가
            stack.append(item.value)
    return stack

def get_operand_type(operand):
    try:
        int(operand)
        return "int"
    except ValueError:
        pass

    try:
        datetime.datetime.strptime(operand, "%Y-%m-$d")
        return "date"
    except ValueError:
        pass
    return "char"

def evaluate_boolean_stack(stack):
    ans = []
    for item in stack:
        if isinstance(item, bool):
            # 불리언 값인 경우 스택에 추가
            ans.append(item)
        elif isinstance(item, list):
            # 리스트인 경우 재귀적으로 평가하여 결과를 스택에 추가
            result = evaluate_boolean_stack(item)
            ans.append(result)
        elif isinstance(item, str) and item.lower() in ('and', 'or'):
            # 연산자인 경우 스택에 추가
            ans.append(item.lower())

        while len(ans) >= 3 and isinstance(ans[-1], bool) and ans[-2] in ('and', 'or') and isinstance(ans[-3], bool):
            # 스택의 마지막 3개 항목이 순서대로 불리언 값, 연산자, 불리언 값인 경우 계산 수행
            operand2 = ans.pop()
            operator = ans.pop()
            operand1 = ans.pop()
            if operator == 'and':
                ans.append(operand1 and operand2)
            elif operator == 'or':
                ans.append(operand1 or operand2)

    return ans[0] if ans else False

def check_where_clause_error(condition, table_name, table_schema):
    if "compare" in condition:
        attribute1, operator, attribute2 = condition['compare']
        table_column_list = table_schema["columns"]
        table_column_name_list = [col["col_name"] for col in table_schema["columns"]]
        table_column_type_list = [col["col_type"] for col in table_schema["columns"]]
        table_column_name_set = set(table_column_name_list)
        # check WhereAmbiguousReference
        if len(attribute1) == 2:
            if attribute1[0] is not None and attribute1[0] != table_name:
                raise WhereTableNotSpecified()
        if len(attribute2) == 2:
            if attribute2[0] is not None and attribute2[0] != table_name:
                raise WhereTableNotSpecified()
        # check WhereColumnNotExist
        if len(attribute1) == 2:
            if attribute1[1] not in table_column_name_set:
                raise WhereColumnNotExist()
        if len(attribute2) == 2:
            if attribute2[1] not in table_column_name_set:
                raise WhereColumnNotExist()

    elif "null" in condition:
        condition_table_name, column_name, null_or_not = condition["null"]
        table_column_name_list = [col["col_name"] for col in table_schema["columns"]]
        table_column_type_list = [col["col_type"] for col in table_schema["columns"]]
        table_column_name_set = set(table_column_name_list)
        # check WhereTableNotSpecified
        if (condition_table_name is not None) and condition_table_name != table_name:
                raise WhereTableNotSpecified()
        # check WhereColumnNotExist
        if column_name not in table_column_name_set:
            raise WhereColumnNotExist()

def evaluate_conditions(condition, table_name, table_schema, row_tuple):
    if "compare" in condition:
        attribute1, operator, attribute2 = condition['compare']
        table_column_list = table_schema["columns"]
        table_column_name_list = [col["col_name"] for col in table_schema["columns"]]
        table_column_type_list = [col["col_type"] for col in table_schema["columns"]]
        table_column_name_set = set(table_column_name_list)
        # check WhereAmbiguousReference
        if len(attribute1) == 2:
            if attribute1[0] is not None and attribute1[0] != table_name:
                raise WhereTableNotSpecified()
        if len(attribute2) == 2:
            if attribute2[0] is not None and attribute2[0] != table_name:
                raise WhereTableNotSpecified()
        # check WhereColumnNotExist
        if len(attribute1) == 2:
            if attribute1[1] not in table_column_name_set:
                raise WhereColumnNotExist()
        if len(attribute2) == 2:
            if attribute2[1] not in table_column_name_set:
                raise WhereColumnNotExist()
        # get operand 1
        if len(attribute1) == 1:
            operand1 = attribute1[0]
            operand1_type = get_operand_type(operand1)
            if operand1_type == "char":
                operand1 = operand1[1:-1]
        else:
            for idx, col_name in enumerate(table_column_name_list):
                if col_name == attribute1[1]:
                    operand1 = row_tuple[1][idx]
                    operand1_type = table_column_type_list[idx]
        # get operand 2
        if len(attribute2) == 1:
            operand2 = attribute2[0]
            operand2_type = get_operand_type(operand2)
            if operand2_type == "char":
                operand2 = operand2[1:-1]
        else:
            for idx, col_name in enumerate(table_column_name_list):
                if col_name == attribute2[1]:
                    operand2 = row_tuple[1][idx]
                    operand2_type = table_column_type_list[idx]
        # check WhereIncomparableError
        if operand1_type != operand2_type:
            raise WhereIncomparableError()
        # evaluate
        if operand1_type == "char":
            if operator == "=":
                return operand1 == operand2
            elif operator == "!=":
                return operand1 != operand2
            elif operator == "<":
                return operand1 < operand2
            elif operator == ">":
                return operand1 > operand2
            elif operator == "<=":
                return operand1 <= operand2
            elif operator == ">=":
                return operand1 >= operand2
        elif operand1_type == "int":
            operand1 = int(operand1)
            operand2 = int(operand2)
            if operator == "=":
                return operand1 == operand2
            elif operator == "!=":
                return operand1 != operand2
            elif operator == "<":
                return operand1 < operand2
            elif operator == ">":
                return operand1 > operand2
            elif operator == "<=":
                return operand1 <= operand2
            elif operator == ">=":
                return operand1 >= operand2
        if operand1_type == "date":
            operand1 = datetime.strptime(operand1, "%Y-%m-%d")
            operand2 = datetime.strptime(operand2, "%Y-%m-%d")
            if operator == "=":
                return operand1 == operand2
            elif operator == "!=":
                return operand1 != operand2
            elif operator == "<":
                return operand1 < operand2
            elif operator == ">":
                return operand1 > operand2
            elif operator == "<=":
                return operand1 <= operand2
            elif operator == ">=":
                return operand1 >= operand2


    elif "null" in condition:
        condition_table_name, column_name, null_or_not = condition["null"]
        table_column_name_list = [col["col_name"] for col in table_schema["columns"]]
        table_column_type_list = [col["col_type"] for col in table_schema["columns"]]
        table_column_name_set = set(table_column_name_list)
        # check WhereTableNotSpecified
        if (condition_table_name is not None) and condition_table_name != table_name:
                raise WhereTableNotSpecified()
        # check WhereColumnNotExist
        if column_name not in table_column_name_set:
            raise WhereColumnNotExist()
        # get operand
        for idx, col_name in enumerate(table_column_name_list):
            if col_name == column_name:
                operand = row_tuple[1][idx]
        if null_or_not.lower() == "not":
            return operand != "null"
        elif null_or_not.lower() == "null":
            return operand == "null"


# Function : select data in table in berkeleydb
def sql_select(sql_data): # todo : implement
    table_name_list = sql_data["referred_table_list"]
    # check SelectTableExistenceError
    for table_name in table_name_list:
        if not (myDB.get(pickle.dumps(table_name))):
            raise SelectTableExistenceError(table_name)
    rows = []
    table_schema_list = []
    column_dict = {}
    column_list_flatten = []
    column_list_check_ambigious = []
    # get all rows at each table
    for table_name in table_name_list:
        table_path = pickle.loads(myDB.get(pickle.dumps(table_name)))
        tableDB = db.DB()
        tableDB.open(table_path, dbtype=db.DB_HASH)
        table_schema = pickle.loads(tableDB.get(b'schema'))
        table_schema_list.append(table_schema)
        table_column_name_list = [col["col_name"] for col in table_schema["columns"]]
        column_dict[table_name] = table_column_name_list
        for column_name in table_column_name_list:
            column_list_flatten.append(table_name+"."+column_name)
            column_list_check_ambigious.append(column_name)
        cursor = tableDB.cursor()
        table_rows = []
        while data := cursor.next():
            if data[0] != b'schema':
                table_rows.append(pickle.loads(data[1]))
        rows.append(table_rows)
    
    cartersian_rows = cartesian_product(rows)
    selected_row = []
    selected_column_list = sql_data["selected_column_list"]

    # check SelectColumnResolveError
    for selected_column in selected_column_list:
        selected_column_name = ""
        if selected_column[0] is None:
            selected_column_name = selected_column[1]
        else:
            selected_column_name = selected_column[0] + "." + selected_column[1]
        # check ambigious, nonEx
        if selected_column[0] is None:
            check = 0
            for column_name_ambigious_check in column_list_check_ambigious:
                if column_name_ambigious_check == selected_column[1]:
                    check += 1
            if check != 1:
                raise SelectColumnResolveError(selected_column_name)
        # check NonEx
        if selected_column[0] is not None:
            if selected_column_name not in set(column_list_flatten):
                raise SelectColumnResolveError(selected_column_name)

    # check where clause
    where_clause = sql_data["where_clause"]
    if where_clause is not None:
        if len(cartersian_rows) == 0:
            replace_with_true_select_for_error_checking(where_clause, table_name_list, table_schema_list, column_list_flatten)
        for cartersian_row in cartersian_rows:
            boolean_stack = replace_with_true_select(where_clause, table_name_list, table_schema_list, column_list_flatten, cartersian_row)
            if evaluate_boolean_stack(boolean_stack):
                selected_row.append(cartersian_row)
    else:
        for cartersian_row in cartersian_rows:
            selected_row.append(cartersian_row)

    # if select *
    if len(table_name_list) > 1:
        if len(selected_column_list) == 0:
            for column_flatten in column_list_flatten:
                selected_column_list.append(column_flatten.split("."))
    else:
        if len(selected_column_list) == 0:
            for column_flatten in column_list_flatten:
                selected_column_list.append((None ,column_flatten.split(".")[1]))

    print_column_name = []
    for selected_column_elem in selected_column_list:
            if selected_column_elem[0] is None:
                column_elem = selected_column_elem[1]
            else:
                column_elem = selected_column_elem[0] + "." + selected_column_elem[1]
            print_column_name.append(column_elem)
    print_row = []
    for row_num, selected_row_elem in enumerate(selected_row):
        selected_data = []
        for selected_column_elem in selected_column_list:
            if selected_column_elem[0] is None:
                column_elem = selected_column_elem[1]
            else:
                column_elem = selected_column_elem[0] + "." + selected_column_elem[1]
            for idx, elem in enumerate(column_list_flatten):
                if column_elem in elem:
                    selected_data.append(selected_row_elem[idx])
        print_row.append(selected_data)
            
    max_length = [len(column) for column in print_column_name]
    for each_print_row in print_row:
        for i, value in enumerate(each_print_row):
            max_length[i] = max(max_length[i], len(str(value)))
    # print using maximum column length
    format_str = "|".join(["{{:<{}}}".format(length) for length in max_length])
    print("+" + "+".join(["-" * length for length in max_length]) + "+")
    print(format_str.format(*print_column_name))
    print("+" + "+".join(["-" * length for length in max_length]) + "+")
    for each_print_row in print_row:
        print(format_str.format(*each_print_row))
    print("+" + "+".join(["-" * length for length in max_length]) + "+")

def replace_with_true_select_for_error_checking(expression, table_name_list, table_schema_list, table_row_list):
    stack = []
    for item in expression:
        if isinstance(item, list):
            # 리스트인 경우 재귀적으로 탐색하여 결과를 스택에 추가
            stack.append(replace_with_true_select(item, table_name_list, table_schema_list, table_row_list))
        elif isinstance(item, dict):
            # 딕셔너리인 경우 `True` 값을 스택에 추가
            stack.append(check_where_clause_error_select(item["predicate"], table_name_list, table_schema_list, table_row_list))
        else:
            # 다른 타입의 항목인 경우 그대로 스택에 추가
            stack.append(item.value)
    return stack

def replace_with_true_select(expression, table_name_list, table_schema_list, table_row_list, rows):
    stack = []
    for item in expression:
        if isinstance(item, list):
            # 리스트인 경우 재귀적으로 탐색하여 결과를 스택에 추가
            stack.append(replace_with_true_select(item, table_name_list, table_schema_list, table_row_list, rows))
        elif isinstance(item, dict):
            # 딕셔너리인 경우 `True` 값을 스택에 추가
            stack.append(evaluate_conditions_select(item["predicate"], table_name_list, table_schema_list, table_row_list, rows))
        else:
            # 다른 타입의 항목인 경우 그대로 스택에 추가
            stack.append(item.value)
    return stack

def check_where_clause_error_select(condition, table_name_list, table_schema_list, table_row_list):
    if "compare" in condition:
        attribute1, operator, attribute2 = condition['compare']
        table_column_type_list = []
        for table_schema in table_schema_list:
            table_column_type = [col["col_type"] for col in table_schema["columns"]]
            for column_type in table_column_type:
                table_column_type_list.append(column_type)
        table_column_name_set = set(table_row_list)
        # check WhereTableNotSpecified
        if len(attribute1) == 2:
            if attribute1[0] is not None and attribute1[0] not in set(table_name_list):
                raise WhereTableNotSpecified()
        if len(attribute2) == 2:
            if attribute2[0] is not None and attribute2[0] not in set(table_name_list):
                raise WhereTableNotSpecified()
        # check WhereColumnNotExist
        if len(attribute1) == 2:
            if attribute1[0] is None:
                attribute_column_name = attribute1[1]
            else:
                attribute_column_name = attribute1[0] + "." + attribute1[1]
                if attribute_column_name not in table_column_name_set:
                    raise WhereColumnNotExist()
        if len(attribute2) == 2:
            if attribute2[0] is None:
                attribute_column_name = attribute2[1]
            else:
                attribute_column_name = attribute2[0] + "." + attribute2[1]
                if attribute_column_name not in table_column_name_set:
                    raise WhereColumnNotExist()
        # check WhereAmbiguousReference
        if len(attribute1) == 2:
            if attribute1[0] is None:
                check = 0
                for table_col_name in table_row_list:
                    if attribute1[1] == table_col_name.split(".")[-1]:
                        check += 1
                if check > 1:
                    raise WhereAmbiguousReference()
                if check == 0:
                    raise WhereColumnNotExist()
        if len(attribute2) == 2:
            if attribute2[0] is None:
                check = 0
                for table_col_name in table_row_list:
                    if attribute2[1] == table_col_name.split(".")[-1]:
                        check += 1
                if check > 1:
                    raise WhereAmbiguousReference()
                if check == 0:
                    raise WhereColumnNotExist() 

    elif "null" in condition:
        condition_table_name, column_name, null_or_not = condition["null"]
        table_column_type_list = []
        for table_schema in table_schema_list:
            table_column_type = [col["col_type"] for col in table_schema["columns"]]
            for column_type in table_column_type:
                table_column_type_list.append(column_type)
        table_column_name_set = set(table_row_list)
        # check WhereTableNotSpecified
        if condition_table_name is not None:
            if condition_table_name not in set(table_name_list):
                raise WhereTableNotSpecified()
        # check WhereColumnNotExist
        attribute_column_name = column_name
        if condition_table_name is not None:
            attribute_column_name = condition_table_name + "." + column_name
            if attribute_column_name not in table_column_name_set:
                raise WhereColumnNotExist()
        # check WhereAmbiguousReference
        if condition_table_name is None:
            attribute_column_name = column_name
            check = 0
            for table_col_name in table_row_list:
                if column_name == table_col_name.split(".")[-1]:
                    check += 1
            if check > 1:
                raise WhereAmbiguousReference()
            if check == 0:
                raise WhereColumnNotExist()

def evaluate_conditions_select(condition, table_name_list, table_schema_list, table_row_list, rows):
    if "compare" in condition:
        attribute1, operator, attribute2 = condition['compare']
        table_column_type_list = []
        for table_schema in table_schema_list:
            table_column_type = [col["col_type"] for col in table_schema["columns"]]
            for column_type in table_column_type:
                table_column_type_list.append(column_type)
        table_column_name_set = set(table_row_list)
        # check WhereTableNotSpecified
        if len(attribute1) == 2:
            if attribute1[0] is not None and attribute1[0] not in set(table_name_list):
                raise WhereTableNotSpecified()
        if len(attribute2) == 2:
            if attribute2[0] is not None and attribute2[0] not in set(table_name_list):
                raise WhereTableNotSpecified()
        # check WhereColumnNotExist
        if len(attribute1) == 2:
            if attribute1[0] is None:
                attribute_column_name = attribute1[1]
            else:
                attribute_column_name = attribute1[0] + "." + attribute1[1]
                if attribute_column_name not in table_column_name_set:
                    raise WhereColumnNotExist()
        if len(attribute2) == 2:
            if attribute2[0] is None:
                attribute_column_name = attribute2[1]
            else:
                attribute_column_name = attribute2[0] + "." + attribute2[1]
                if attribute_column_name not in table_column_name_set:
                    raise WhereColumnNotExist()
        # check WhereAmbiguousReference
        if len(attribute1) == 2:
            if attribute1[0] is None:
                check = 0
                for table_col_name in table_row_list:
                    if attribute1[1] == table_col_name.split(".")[-1]:
                        check += 1
                if check > 1:
                    raise WhereAmbiguousReference()
                if check == 0:
                    raise WhereColumnNotExist()
        if len(attribute2) == 2:
            if attribute2[0] is None:
                check = 0
                for table_col_name in table_row_list:
                    if attribute2[1] == table_col_name.split(".")[-1]:
                        check += 1
                if check > 1:
                    raise WhereAmbiguousReference()
                if check == 0:
                    raise WhereColumnNotExist() 
        # get operand 1
        if len(attribute1) == 1:
            operand1 = attribute1[0]
            operand1_type = get_operand_type(operand1)
            if operand1_type == "char":
                operand1 = operand1[1:-1]
        else:
            if attribute1[0] is None:
                operand_column_name = attribute1[1]
            else:
                operand_column_name = attribute1[0] + "." + attribute1[1]
            for idx, col_name in enumerate(table_row_list):
                if operand_column_name in col_name:
                    operand1 = rows[idx]
                    operand1_type = table_column_type_list[idx]
        # get operand 2
        if len(attribute2) == 1:
            operand2 = attribute2[0]
            operand2_type = get_operand_type(operand2)
            if operand2_type == "char":
                operand2 = operand2[1:-1]
        else:
            if attribute2[0] is None:
                operand_column_name = attribute2[1]
            else:
                operand_column_name = attribute2[0] + "." + attribute2[1]
            for idx, col_name in enumerate(table_row_list):
                if operand_column_name in col_name:
                    operand2 = rows[idx]
                    operand2_type = table_column_type_list[idx]
        # check WhereIncomparableError
        if operand1_type != operand2_type:
            raise WhereIncomparableError()
        # evaluate
        if operand1_type == "char":
            if operator == "=":
                return operand1 == operand2
            elif operator == "!=":
                return operand1 != operand2
            elif operator == "<":
                return operand1 < operand2
            elif operator == ">":
                return operand1 > operand2
            elif operator == "<=":
                return operand1 <= operand2
            elif operator == ">=":
                return operand1 >= operand2
        elif operand1_type == "int":
            operand1 = int(operand1)
            operand2 = int(operand2)
            if operator == "=":
                return operand1 == operand2
            elif operator == "!=":
                return operand1 != operand2
            elif operator == "<":
                return operand1 < operand2
            elif operator == ">":
                return operand1 > operand2
            elif operator == "<=":
                return operand1 <= operand2
            elif operator == ">=":
                return operand1 >= operand2
        if operand1_type == "date":
            operand1 = datetime.strptime(operand1, "%Y-%m-%d")
            operand2 = datetime.strptime(operand2, "%Y-%m-%d")
            if operator == "=":
                return operand1 == operand2
            elif operator == "!=":
                return operand1 != operand2
            elif operator == "<":
                return operand1 < operand2
            elif operator == ">":
                return operand1 > operand2
            elif operator == "<=":
                return operand1 <= operand2
            elif operator == ">=":
                return operand1 >= operand2

    elif "null" in condition:
        condition_table_name, column_name, null_or_not = condition["null"]
        table_column_type_list = []
        for table_schema in table_schema_list:
            table_column_type = [col["col_type"] for col in table_schema["columns"]]
            for column_type in table_column_type:
                table_column_type_list.append(column_type)
        table_column_name_set = set(table_row_list)
        # check WhereTableNotSpecified
        if condition_table_name is not None:
            if condition_table_name not in set(table_name_list):
                raise WhereTableNotSpecified()
        # check WhereColumnNotExist
        attribute_column_name = column_name
        if condition_table_name is not None:
            attribute_column_name = condition_table_name + "." + column_name
            if attribute_column_name not in table_column_name_set:
                raise WhereColumnNotExist()
        # check WhereAmbiguousReference
        if condition_table_name is None:
            attribute_column_name = column_name
            check = 0
            for table_col_name in table_row_list:
                if column_name == table_col_name.split(".")[-1]:
                    check += 1
            if check > 1:
                raise WhereAmbiguousReference()
            if check == 0:
                raise WhereColumnNotExist()
        # get operand
        for idx, col_name in enumerate(table_row_list):
            if attribute_column_name in col_name:
                operand = rows[idx]
        # print(operand)
        if null_or_not.lower() == "not":
            return operand != "null"
        elif null_or_not.lower() == "null":
            return operand == "null"

def cartesian_product(lists):
    result = []
    for items in itertools.product(*lists):
        flattened = [item for sublist in items for item in sublist]
        result.append(flattened)
    return result

# Function : show table list in berkeleydb
def sql_show_tables(sql_data):
    cursor = myDB.cursor()
    print('-' * 65)
    while x := cursor.next():
        print(pickle.loads(x[0]))
    print('-' * 65)

# Function : update data in table in berkeleydb    
def sql_update(sql_data): # !Not implemented yet
    sql_data

# Fuction : sql runner driver
def sql_runner(sql_type, sql_data):
    if sql_type == "CREATE TABLE": # when type is create
        sql_create_table(sql_data)
    elif sql_type == "DROP TABLE": # when type is drop
        sql_drop_table(sql_data)
    elif sql_type == "EXPLAIN": # when type is explain
        sql_explain(sql_data)
    elif sql_type == "DESCRIBE": # when type is describe
        sql_explain(sql_data)
    elif sql_type == "DESC": # when type is desc
        sql_explain(sql_data)
    elif sql_type == "INSERT": # when type is insert
        sql_insert(sql_data)
    elif sql_type == "DELETE": # when type is delete
        sql_delete(sql_data)
    elif sql_type == "SELECT": # when type is select
        sql_select(sql_data)
    elif sql_type == "SHOW TABLES": # when type is show tables
        sql_show_tables(sql_data)
    elif sql_type == "UPDATE": # when type is update
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
        except (Exception) as e:
            print(e) # if error occurred, print error message
            break