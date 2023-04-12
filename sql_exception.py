""" 
error.py
 - Define Error class for DBMS(run.py)

Error List (Error Class - Error Message)
 - SyntaxError : Syntax error
 - DuplicateColumnDefError - Create table has failed: column definition is duplicated
 - DuplicatePrimaryKeyDefError - Create table has failed: primary key definition is duplicated
 - ReferenceTypeError - Create table has failed: foreign key references wrong type
 - ReferenceNonPrimaryKeyError - Create table has failed: foreign key references non primary key column
 - ReferenceColumnExistenceError - Create table has failed: foreign key references non existing column
 - ReferenceTableExistenceError - Create table has failed: foreign key references non existing table
 - NonExistingColumnDefError(#colName) - Create table has failed: '[#colName]' does not exist in column definition
 - TableExistenceError - Create table has failed: table with the same name already exists
 - CharLengthError - Char length should be over 0
 - NoSuchTable - No such table
 - DropReferencedTableError(#tableName) - Drop table has failed: '#tableName' is referenced by other table
 - SelectTableExistenceError(#tableName) - Selection has failed: '#tableName' does not exist
"""

class SyntaxError(Exception):
    def __init__(self):
        super()

    def __str__(self):
        return "Syntax error"
    
class DuplicateColumnDefError(Exception):
    def __init__(self):
        super()

    def __str__(self):
        return "Create table has failed: column definition is duplicated"
    
class DuplicatePrimaryKeyDefError(Exception):
    def __init__(self):
        super()

    def __str__(self):
        return "Create table has failed: primary key definition is duplicated"
    
class ReferenceTypeError(Exception):
    def __init__(self):
        super()

    def __str__(self):
        return "Create table has failed: foreign key references wrong type"

class ReferenceNonPrimaryKeyError(Exception):
    def __init__(self):
        super()

    def __str__(self):
        return "Create table has failed: foreign key references non primary key column"
    
class NonExistingColumnDefError(Exception):
    def __init__(self, colName):
        super()
        self.colName = colName

    def __str__(self):
        return f"Create table has failed: {self.colName} does not exist in column definition"
    
class TableExistenceError(Exception):
    def __init__(self):
        super()

    def __str__(self):
        return "Create table has failed: table with the same name already exists"


class CharLengthError(Exception):
    def __init__(self):
        super()

    def __str__(self):
        return "Char length should be over 0"
    
class NoSuchTable(Exception):
    def __init__(self):
        super()

    def __str__(self):
        return "No such table"
    
class DropReferencedTableError(Exception):
    def __init__(self, tableName):
        super()
        self.tableName = tableName

    def __str__(self):
        return f"Drop table has failed: {self.tableName} is referenced by other table"
    
class SelectTableExistenceError(Exception):
    def __init__(self, tableName):
        super()
        self.tableName = tableName

    def __str__(self):
        return f"Selection has failed: {self.tableName} does not exist"