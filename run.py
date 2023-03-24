import lark

# tree 탐색하면서 sql type을 리턴 
class sqlTransformer(lark.Transformer):
    def __init__(self):
        self.sql_type = ""

    def command(self, args):
        if args[0] == "exit":
            return "exit"
        else:
            return self.sql_type

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

# prompt로부터 문장들을 입력받아, ;기준으로 split하여 list에 저장
def getSqlLIST():
    sentence_in = input("DB_2020-12907 > ").rstrip()
    while sentence_in[-1] != ";": # 맨 마지막이 ;로 입력될때까지, 문자열 저장
        sentence_in += " " + input().rstrip()
    sentence_in = sentence_in.replace("\n", "") # 개행문자 제거(\n)
    sentence_in = sentence_in.replace("\r", "") # 개행문자 제거(\r)
    sentence_parsed = sentence_in.strip().split(";") # sql list 생성
    sentence_parsed = sentence_parsed[:-1]
    for i in range(len(sentence_parsed)):
        sentence_parsed[i] = sentence_parsed[i].strip() + ";"
    return sentence_parsed

# 문장을 입력받아 파싱하고 트리 리턴. 단, 에러가 발생하면 False return.
def getParsedSql(sql_sentence):
    try:
        output = sql_parser.parse(sql_sentence)
    except:
        return False
    else:
        return output

# sqlTransformer 인스턴스 생성 및 prompt 유지 flag boolean 생성
sqlTF = sqlTransformer()
flag = True

while flag:
    parsing_list = getSqlLIST()
    for sql_sentence in parsing_list:
        parsed_output = getParsedSql(sql_sentence)
        if parsed_output:
            result = sqlTF.transform(parsed_output)
            if result == "exit":
                flag = False
                break
            print("'{0}' requested".format(result))
        else:
            print("Syntax error")
            break