import sys
import lark

class sqlTransformer(lark.Transformer):
    def __init__(self):
        self.sql_type = ""
    
    def command(self, args):
        if args[0] == "exit":
            return "exit"
        else:
            return self.sql_dict
    
    def query_list(self, args):
        return args
    
    def query(self, args):
        return args
    
    def create_table_query(self, args):
        self.sql_type = "CREATE_TABLE"
        return
    
    def drop_table_query(self, args):
        self.sql_type = "DROP_TABLE"
        return
    
    def explain_table_query(self, args):
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
    
    def update_query(self, args):
        self.sql_type = "UPDATE"
        return
    


with open("grammar.lark") as file:
    sql_parser = lark.Lark(file.read(), start="command", lexer="basic")

def testAllExQuery():
    #test1 create
    output = sql_parser.parse("create table account (account_number int not null, branch_name char(15), primary key(account_number));")
    print(output.pretty())

    #test2 Drop
    output = sql_parser.parse("drop table table_name;")
    print(output.pretty())

    #test3 Explain, Describe, Desc
    output = sql_parser.parse("explain account;")
    print(output.pretty())
    output = sql_parser.parse("describe account;")
    print(output.pretty())
    output = sql_parser.parse("desc account;")
    print(output.pretty())

    #test4 insert
    output = sql_parser.parse("insert into account values(9732, 'Perryridge');")
    print(output.pretty())

    #test5 delete
    output = sql_parser.parse("delete from account where branch_name = 'Perryridge';")
    print(output.pretty())

    #test6 select
    output = sql_parser.parse("select customer_name, borrower.loan_number, amount from borrower, loan where borrower.loan_number = loan.loan_number and branch_name = 'Perryridge'; ")
    print(output.pretty())

    #test7 show
    output = sql_parser.parse("show tables;")
    print(output.pretty())

    #test8 update
    output = sql_parser.parse("update student set id = 5 where name='susan';")
    print(output.pretty())

    #test9 exit
    output = sql_parser.parse("exit;")
    print(output.pretty())

def getSQL():
    senetence_in = input("DB_2020-12907 > ")
    while senetence_in[-1] != ";":
        senetence_in += " " + input()
    sentence_parsed = senetence_in.split(";")
    for i in range(len(sentence_parsed)):
        sentence_parsed[i] = sentence_parsed[i] + ";"
    return sentence_parsed

testAllExQuery()

# def returnParseResult(query):
#     try:
#         result = sql_parser.parse(query)
#     except:

#     else:

#todo 클래스 다 완성되면 아래 주석 풀고 구현
# sqlTF = sqlTransformer()
# sql_list = getSQL()
# output = sql_parser.parse(sql_list[0])
# result = sqlTF.transform(output)
# print("'" + result["sql_type"] + "'" + " requested")

# while True:
#     querys = input()
#     output = sql_parser.parse(querys)
#     print(output.pretty())