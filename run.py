import lark

# Class : Using lark.transformer, bottom-up query searching 
class sqlTransformer(lark.Transformer):
    def __init__(self):
        self.sql_type = ""

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

# SQL Parser using grammar.lark
with open("grammar.lark") as file:
    sql_parser = lark.Lark(file.read(), start="command", lexer="basic")

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
        output = sql_parser.parse(sql_sentence)
    except:
        return False
    else:
        return output

# Make sqlTransformer instance and program repeating flag
sqlTF = sqlTransformer()
flag = True

# Main program : parsing sql until get 'exit;'
while flag:
    parsing_list = getSqlLIST()
    for sql_sentence in parsing_list:
        parsed_output = getParsedSql(sql_sentence) # get sql tree
        if parsed_output:
            result = sqlTF.transform(parsed_output) # get sql type
            if result == "exit": # if 'exit;' then break program
                flag = False
                break
            print("DB_2020-12907> '{0}' requested".format(result)) # if correct syntax, print sql type
        else:
            print("DB_2020-12907> Syntax error") # if wrong syntax, pirnt 'syntax error'
            break