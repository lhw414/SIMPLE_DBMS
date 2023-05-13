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
 - InsertTypeMismatchError - Insertion has failed: Types are not matched
 - InsertColumnExistenceError(#colName) - Insertion has failed: '#colName' does not exist
 - InsertColumnNonNullableError(#colName) - Insertion has failed: '#colName' is not nullable
 - SelectTableExistenceError(#tableName) - Selection has failed: '#tableName' does not exist
 - SelectColumnResolveError(#colName) - Selection has failed: fail to resolve '#colName'
 - WhereIncomparableError - Where clause trying to compare incomparable values
 - WhereTableNotSpecified - Where clause trying to reference tables which are not specified
 - WhereColumnNotExist - Where clause trying to reference non existing column
 - WhereAmbiguousReference - Where clause contains ambiguous reference
"""

class SyntaxError(Exception):
    def __init__(self):
        super()

    def __str__(self):
        return "DB_2020-12907> Syntax error"
    
class DuplicateColumnDefError(Exception):
    def __init__(self):
        super()

    def __str__(self):
        return "DB_2020-12907> Create table has failed: column definition is duplicated"
    
class DuplicatePrimaryKeyDefError(Exception):
    def __init__(self):
        super()

    def __str__(self):
        return "DB_2020-12907> Create table has failed: primary key definition is duplicated"
    
class ReferenceTypeError(Exception):
    def __init__(self):
        super()

    def __str__(self):
        return "DB_2020-12907> Create table has failed: foreign key references wrong type"

class ReferenceNonPrimaryKeyError(Exception):
    def __init__(self):
        super()

    def __str__(self):
        return "DB_2020-12907> Create table has failed: foreign key references non primary key column"
    
class ReferenceColumnExistenceError(Exception):
    def __init__(self):
        super()

    def __str__(self):
        return "DB_2020-12907> Create table has failed: foreign key references non existing column"
    
class ReferenceTableExistenceError(Exception):
    def __init__(self):
        super()

    def __str__(self):
        return "DB_2020-12907> Create table has failed: foreign key references non existing table"
    
class NonExistingColumnDefError(Exception):
    def __init__(self, colName):
        super()
        self.colName = colName

    def __str__(self):
        return f"DB_2020-12907> Create table has failed: '{self.colName}' does not exist in column definition"
    
class TableExistenceError(Exception):
    def __init__(self):
        super()

    def __str__(self):
        return "DB_2020-12907> Create table has failed: table with the same name already exists"


class CharLengthError(Exception):
    def __init__(self):
        super()

    def __str__(self):
        return "DB_2020-12907> Char length should be over 0"
    
class NoSuchTable(Exception):
    def __init__(self):
        super()

    def __str__(self):
        return "DB_2020-12907> No such table"
    
class DropReferencedTableError(Exception):
    def __init__(self, tableName):
        super()
        self.tableName = tableName

    def __str__(self):
        return f"DB_2020-12907> Drop table has failed: '{self.tableName}' is referenced by other table"
    
class mydbCreateError(Exception):
    def __init__(self, *args: object):
        super()

    def __str__(self):
        return "DB_2020-12907> Do not create 'myDB' table"
    
class InsertTypeMismatchError(Exception):
    def __init__(self):
        super()

    def __str__(self):
        return f"DB_2020-12907> Insertion has failed: Types are not matched"
    
class InsertColumnExistenceError(Exception):
    def __init__(self, colName):
        super()
        self.colName = colName

    def __str__(self):
        return f"DB_2020-12907> Insertion has failed: '{self.colName}' does not exist"
    
class InsertColumnNonNullableError(Exception):
    def __init__(self, colName):
        super()
        self.colName = colName

    def __str__(self):
        return f"DB_2020-12907> Insertion has failed: {self.colName} is not nullable"
    
class SelectTableExistenceError(Exception):
    def __init__(self, tableName):
        super()
        self.tableName = tableName;

    def __str__(self):
        return f"DB_2020-12907> Selection has failed: '{self.tableName}' does not exist"
    
class SelectColumnResolveError(Exception):
    def __init__(self, colName):
        super()
        self.colName = colName;

    def __str__(self):
        return f"DB_2020-12907> Selection has failed: fail to resolve '{self.colName}'"
    
class WhereIncomparableError(Exception):
    def __init__(self):
        super()

    def __str__(self):
        return f"DB_2020-12907> Where clause trying to compare incomparable values"
    
class WhereTableNotSpecified(Exception):
    def __init__(self):
        super()

    def __str__(self):
        return f"DB_2020-12907> Where clause trying to reference tables which are not specified"
    
class WhereColumnNotExist(Exception):
    def __init__(self):
        super()

    def __str__(self):
        return f"DB_2020-12907> Where clause trying to reference non existing column"
    
class WhereAmbiguousReference(Exception):
    def __init__(self):
        super()

    def __str__(self):
        return f"DB_2020-12907> Where clause contains ambiguous reference"