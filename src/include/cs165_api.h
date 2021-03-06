

/* BREAK APART THIS API (TODO MYSELF) */
/* PLEASE UPPERCASE ALL THE STUCTS */

/*
Copyright (c) 2015 Harvard University - Data Systems Laboratory (DASLab)
Permission is hereby granted, free of charge, to any person obtaining a copy of
this software and associated documentation files (the "Software"), to deal in
the Software without restriction, including without limitation the rights to
use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies
of the Software, and to permit persons to whom the Software is furnished to do
so, subject to the following conditions:
The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.
THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.
*/

#ifndef CS165_H
#define CS165_H

#include <stdlib.h>
#include <stdbool.h>
#include <stdio.h>
#include <sys/time.h>
#include "common.h"


/**
 * EXTRA
 * DataType
 * Flag to mark what type of data is held in the struct.
 * You can support additional types by including this enum and using void*
 * in place of int* in db_operator simliar to the way IndexType supports
 * additional types.
 **/

typedef enum DataType {
     INT,
     LONG,
     FLOAT,
     DOUBLE
} DataType;

typedef enum IndexType{
    NONE,
    SORTED,
    BTREE
}IndexType;

struct Comparator;

typedef struct dataRecord{
    int pos;
    int val;
}dataRecord;

typedef struct ColumnIndex
{
    IndexType indexType;
    void* dataIndex;
    bool clustered;
    bool unclustered;
    size_t index_data_capacity;
    dataRecord* tuples;
}ColumnIndex;

typedef struct Column {
    char name[MAX_SIZE_NAME]; 
    int* data;    
    ColumnIndex *index;
} Column;

/**
 * table
 * Defines a table structure, which is composed of multiple columns.
 * We do not require you to dynamically manage the size of your tables,
 * although you are free to append to the struct if you would like to (i.e.,
 * include a size_t table_size).
 * name, the name associated with the table. table names must be unique
 *     within a database, but tables from different databases can have the same
 *     name.
 * - col_count, the number of columns in the table
 * - col,umns this is the pointer to an array of columns contained in the table.
 * - table_length, the size of the columns in the table.
 **/

typedef struct Table {
    char name [MAX_SIZE_NAME];
    Column *columns;
    size_t col_count;
    size_t table_length;
    size_t col_data_capacity;
    char firstDeclaredClustCol[MAX_SIZE_NAME];
} Table;

/**
 * db
 * Defines a database structure, which is composed of multiple tables.
 * - name: the name of the associated database.
 * - tables: the pointer to the array of tables contained in the db.
 * - tables_size: the size of the array holding table objects
 * - tables_capacity: the amount of pointers that can be held in the currently allocated memory slot
 **/

typedef struct Db {
    char name[MAX_SIZE_NAME]; 
    Table *tables;
    size_t tables_size;
    size_t tables_capacity;
} Db;

/**
 * Error codes used to indicate the outcome of an API call
 **/
typedef enum StatusCode {
  /* The operation completed successfully */
  OK,
  /* There was an error with the call. */
  ERROR,
} StatusCode;

// status declares an error code and associated message
typedef struct Status {
    StatusCode code;
    char* error_message;
} Status;

// Defines a comparator flag between two values.
typedef enum ComparatorType {
    NO_COMPARISON = 0,
    LESS_THAN = 1,
    GREATER_THAN = 2,
    EQUAL = 4,
    LESS_THAN_OR_EQUAL = 5,
    GREATER_THAN_OR_EQUAL = 6
} ComparatorType;

/*
 * Declares the type of a result column, 
 which includes the number of tuples in the result, the data type of the result, and a pointer to the result data
 */
typedef struct Result {
    size_t num_tuples;
    DataType data_type;
    void *payload;
    int lower_idx;
    int upper_idx;
} Result;

/*
 * an enum which allows us to differentiate between columns and results
 */
typedef enum GeneralizedColumnType {
    RESULT,
    COLUMN
} GeneralizedColumnType;
/*
 * a union type holding either a column or a result struct
 */
typedef union GeneralizedColumnPointer {
    Result* result;
    Column* column;
} GeneralizedColumnPointer;

/*
 * unifying type holding either a column or a result
 */
typedef struct GeneralizedColumn {
    GeneralizedColumnType column_type;
    GeneralizedColumnPointer column_pointer;
} GeneralizedColumn;

/*
 * used to refer to a column in our client context
 */

typedef struct GeneralizedColumnHandle {
    char name[HANDLE_MAX_SIZE];
    GeneralizedColumn generalized_column;
} GeneralizedColumnHandle;


/**
 * comparator
 * A comparator defines a comparison operation over a column. 
 **/
typedef struct Comparator {
    long int p_low; // used in equality and ranges.
    long int p_high; // used in range compares. 
    GeneralizedColumn* gen_col;
    size_t numInputCols;
    ComparatorType type1;
    ComparatorType type2;
    char* handle;
} Comparator;

/*
 * tells the databaase what type of operator this is
 */
typedef enum OperatorType {
    CREATE,
    CREATE_IDX,
    INSERT,
    OPEN,
    SELECT, 
    BATCH,
    FETCH, 
    PRINT, 
    AVG_SUM,
    ADD_SUB,
    MAX_MIN,
    JOIN,
    UPDATE,
    DELETE
} OperatorType;

typedef struct DeleteOperator {
    Table* table;
    GeneralizedColumn* gen_res_col;
} DeleteOperator;

typedef struct UpdateOperator {
    Table* table;
    Column* column;
    GeneralizedColumn* gen_res_col;
    int update_val;
} UpdateOperator;
/*
 * necessary fields for insertion
 */
typedef struct InsertOperator {
    Table* table;
    int* values;
} InsertOperator;

typedef struct CreateIdxOperator {
    Table* table;
    Column* column;
} CreateIdxOperator;
/*
 * necessary fields for Open
 */
typedef struct OpenOperator {
    char* db_name;
} OpenOperator;
/*
 * necessary fields for select
 */
typedef struct SelectOperator {
    Comparator* comparator;
    Table* table;
} SelectOperator;

typedef struct BatchOperator {
    int numSelOperators; 
    SelectOperator* selOperators;
} BatchOperator;
/*
 * necessary fields for fetch
 */
typedef struct FetchOperator {
    Column* column;
    char* targetVecHandle;
    char* handle;
} FetchOperator;
/*
 * necessary fields for print
 */
typedef struct PrintOperator {
    char* handleNames[MAX_PRINT_HANDLE];
    int numHandles;
} PrintOperator;
/*
 * necessary fields for avg/sum
 */
typedef struct AvgSumOperator {
    GeneralizedColumn* gen_col;
    char* handle;
    size_t num_data;
    bool isSum;
} AvgSumOperator;
/*
 * necessary fields for add/sub
 */
typedef struct AddSubOperator {
    GeneralizedColumn* gen_col1;
    GeneralizedColumn* gen_col2;
    char* handle;
    size_t num_data;
    bool isAdd;
} AddSubOperator;
/*
 * necessary fields for add/sub
 */
typedef struct MaxMinOperator {
    GeneralizedColumn* gen_col;
    char* handle;
    size_t num_data;
    bool isMax;
} MaxMinOperator;

/*Join Operator */
typedef struct JoinOperator {
    GeneralizedColumn* gen_col;
    char outhandle1[10];
    char outhandle2[10];
    char joinMethod[20];
} JoinOperator;

/*
 * union type holding the fields of any operator
 */
typedef union OperatorFields {
    InsertOperator insert_operator;
    CreateIdxOperator create_idx_operator;
    OpenOperator open_operator;
    SelectOperator select_operator;
    FetchOperator fetch_operator;
    PrintOperator print_operator;
    AvgSumOperator avg_sum_operator;
    AddSubOperator add_sub_operator;
    MaxMinOperator max_min_operator;
    JoinOperator join_operator;
    UpdateOperator update_operator;
    DeleteOperator delete_operator;
} OperatorFields;

/*
 * holds the information necessary to refer to generalized columns (results or columns)
 */
typedef struct ClientContext {
    GeneralizedColumnHandle* chandle_table;
    int chandles_in_use;
    int chandle_slots;
    BatchOperator* batchOperator;
    struct timeval stop;
    struct timeval start;
} ClientContext;

/*
 * DbOperator holds the following fields:
 * type: the type of operator to perform (i.e. insert, select, ...)
 * operator fields: the fields of the operator in question
 * client_fd: the file descriptor of the client that this operator will return to
 * context: the context of the operator in question. This context holds the local results of the client in question.
 */
typedef struct DbOperator {
    OperatorType type;
    OperatorFields operator_fields;
    int client_fd;
    ClientContext* context;
} DbOperator;

typedef struct Catalog
{
    char dbName[MAX_SIZE_NAME];
    size_t numTables;
    size_t numTableCapacity;
    char tableNames[MAX_TABLE_SIZE] [MAX_SIZE_NAME];
    size_t numTableColumns[MAX_TABLE_SIZE];
    size_t columnSize[MAX_TABLE_SIZE];
    size_t columnDataCapacity[MAX_TABLE_SIZE];
    char columnNames[MAX_TABLE_SIZE][MAX_COLUMN_SIZE][MAX_SIZE_NAME];
    char firstDeclaredClustCol[MAX_TABLE_SIZE] [MAX_SIZE_NAME];
    size_t indexDataCapacity[MAX_TABLE_SIZE];
    char columnIndexType[MAX_TABLE_SIZE][MAX_COLUMN_SIZE][MAX_SIZE_NAME];
    char columnIndexClustType[MAX_TABLE_SIZE][MAX_COLUMN_SIZE][MAX_SIZE_NAME];
}Catalog;



extern Db *current_db;
 
Status db_startup();

/**
 * sync_db(db)
 * Saves the current status of the database to disk.
 *
 * db       : the database to sync.
 * returns  : the status of the operation.
 **/
Status sync_db(Db* db);

void add_db(const char* db_name, bool new, Status *status);

Table* create_table(Db* db, const char* name, size_t num_columns, Status *status);

Column* create_column(char *name, Table *table, bool sorted, Status *ret_status);

Status shutdown_server();
Status shutdown_database(Db* db);
Status saveDatabase();

char** execute_db_operator(DbOperator* query);
void db_operator_free(DbOperator* query);

void execute_DbOperator(DbOperator* query, char** msg);

#endif /* CS165_H */

