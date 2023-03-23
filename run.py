import lark


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
    senetence_in = input("DB_2020-12907 > ")
    while senetence_in[-1] != ";":
        senetence_in += " " + input()
    senetence_in.replace("\n", "")
    senetence_in.replace("\r", "")
    sentence_parsed = senetence_in.split(";")
    sentence_parsed = sentence_parsed[:-1]
    for i in range(len(sentence_parsed)):
        sentence_parsed[i] = sentence_parsed[i].strip() + ";"
    return sentence_parsed

#
def getParsedSql(sql_sentence):
    try:
        output = sql_parser.parse(sql_sentence)
    except:
        return False
    else:
        return output

sqlTF = sqlTransformer()
flag = True

while flag:
    parsing_list = getSqlLIST()
    print(parsing_list)
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