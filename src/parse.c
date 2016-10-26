#define _BSD_SOURCE
#include <string.h>
#include <stdbool.h>
#include <stddef.h>
#include <stdlib.h>
#include <ctype.h>
#include "cs165_api.h"
#include "parse.h"
#include "utils.h"
#include "client_context.h"

/**
 * Takes a pointer to a string.
 * This method returns the original string truncated to where its first comma lies.
 * In addition, the original string now points to the first character after that comma.
 * This method destroys its input.
 **/

char* next_token(char** tokenizer, message_status* status) {
    char* token = strsep(tokenizer, ",");
    if (token == NULL) {
        *status= INCORRECT_FORMAT;
    }
    return token;
}

/**
 * parse_create_idx
 **/
message_status parse_create_idx(char* create_arguments) {
    message_status status = OK_DONE;
    char** create_arguments_index = &create_arguments;
    char* db_tbl_col_name = next_token(create_arguments_index, &status);
    char* idx_type = next_token(create_arguments_index, &status);
    char* cluster_type = next_token(create_arguments_index, &status);

    int last_char = strlen(cluster_type) - 1;
    if (cluster_type[last_char] != ')') {
        return INCORRECT_FORMAT;
    }
    cluster_type[last_char] = '\0';

    char* db_name = strsep(&db_tbl_col_name, ".");
    (void)db_name;
    char* tbl_name = strsep(&db_tbl_col_name, ".");
    char* col_name = db_tbl_col_name;

    Table* table = lookup_table(tbl_name);
    if (table == NULL) {
        status = OBJECT_NOT_FOUND;
    }
    else{
        Column* column = retrieve_column_for_scan(table, col_name, false);
        if (column == NULL) {
            status = OBJECT_NOT_FOUND;
        }else{
            column->index = (ColumnIndex*)malloc(sizeof(ColumnIndex));
            memset(column->index, 0, sizeof(ColumnIndex));

            ColumnIndex* pIndex = column->index;
            if(pIndex != NULL){
                if(strcmp(idx_type, "sorted") == 0)
                    pIndex->indexType = SORTED;
                else if(strcmp(idx_type, "btree") == 0)
                    pIndex->indexType = BTREE;
                else
                    status = INCORRECT_FORMAT;

                if(strcmp(cluster_type, "clustered") == 0){
                    pIndex->clustered = true;
                    pIndex->unclustered = false;
                    if(strcmp(table->firstDeclaredClustCol, "") == 0)
                        strcpy(table->firstDeclaredClustCol, column->name);
                }
                else if(strcmp(cluster_type, "unclustered") == 0){
                    pIndex->unclustered = true;
                    pIndex->clustered = false;
                }
                else
                    status = INCORRECT_FORMAT;

                //Initialize the tuples
                pIndex->index_data_capacity = 200;
                pIndex->tuples = (dataRecord*)malloc(sizeof(dataRecord) * (pIndex->index_data_capacity));
                memset(pIndex->tuples, 0, sizeof(dataRecord)*(pIndex->index_data_capacity));

                status = OK_WAIT_FOR_RESPONSE;
            }
            else
                status = OBJECT_NOT_FOUND;
        }

    }
    return status;
}

/**
 * parse_create_col
 **/
message_status parse_create_col(char* create_arguments) {
    message_status status = OK_DONE;
    char** create_arguments_index = &create_arguments;
    char* col_name = next_token(create_arguments_index, &status);
    char* db_tbl_name = next_token(create_arguments_index, &status);

    col_name = trim_quotes(col_name);
    // not enough arguments
    if (status == INCORRECT_FORMAT) {
        return status;
    }

    int last_char = strlen(db_tbl_name) - 1;
    if (db_tbl_name[last_char] != ')') {
        return INCORRECT_FORMAT;
    }
    db_tbl_name[last_char] = '\0';

    strsep(&db_tbl_name, ".");
    char* table_name = db_tbl_name;

    Table* table = lookup_table(table_name);
    if (table == NULL) {
        status = OBJECT_NOT_FOUND;
    }
    else{
        Status create_col_status;
        create_column(col_name, table, false, &create_col_status);
        if (create_col_status.code != OK) {
            cs165_log(stdout, "adding a table failed.");
            return EXECUTION_ERROR;
        }
        else
            status = OK_WAIT_FOR_RESPONSE;
    }

    return status;
}

/**
 * parse_create_tbl
 * This method takes in a string representing the arguments to create a table.
 * It parses those arguments, checks that they are valid, and creates a table.
 **/

message_status parse_create_tbl(char* create_arguments) {
    message_status status = OK_DONE;
    char** create_arguments_index = &create_arguments;
    char* table_name = next_token(create_arguments_index, &status);
    char* db_name = next_token(create_arguments_index, &status);
    char* col_cnt = next_token(create_arguments_index, &status);

    table_name = trim_quotes(table_name);
    // not enough arguments
    if (status == INCORRECT_FORMAT) {
        return status;
    }

    // read and chop off last char
    int last_char = strlen(col_cnt) - 1;
    if (col_cnt[last_char] != ')') {
        return INCORRECT_FORMAT;
    }
    col_cnt[last_char] = '\0';

    // check that the database argument is the current active database
    if (strcmp(current_db->name, db_name) != 0) {
        cs165_log(stdout, "query unsupported. Bad db name");
        return QUERY_UNSUPPORTED;
    }

    int column_cnt = atoi(col_cnt);
    if (column_cnt < 1) {
        return INCORRECT_FORMAT;
    }
    Status create_status;
    create_table(current_db, table_name, column_cnt, &create_status);
    if (create_status.code != OK) {
        cs165_log(stdout, "adding a table failed.");
        return EXECUTION_ERROR;
    }
    else
        status = OK_WAIT_FOR_RESPONSE;

    return status;
}

/**
 * This method takes in a string representing the arguments to create a database.
 * It parses those arguments, checks that they are valid, and creates a database.
 **/

message_status parse_create_db(char* create_arguments) {
    char *token;
    token = strsep(&create_arguments, ",");
    if (token == NULL) {
        return INCORRECT_FORMAT;                    
    } else {
        // create the database with given name
        char* db_name = token;
        db_name = trim_quotes(db_name);
        int last_char = strlen(db_name) - 1;
        if (last_char < 0 || db_name[last_char] != ')') {
            return INCORRECT_FORMAT;
        }
        db_name[last_char] = '\0';
        token = strsep(&create_arguments, ",");
        if (token != NULL) {
            return INCORRECT_FORMAT;
        }

        Status ret_Status;
        ret_Status.code = ERROR;

        add_db(db_name, true, &ret_Status);
        if (ret_Status.code != ERROR) {
            return OK_WAIT_FOR_RESPONSE;
        } else {
            return EXECUTION_ERROR;
        }
    }
}

/**
 * parse_create parses a create statement and then passes the necessary arguments off to the next function
 **/
message_status parse_create(char* create_arguments) {
    message_status mes_status;
    char *tokenizer_copy, *to_free;
    tokenizer_copy = to_free = malloc((strlen(create_arguments)+1) * sizeof(char));
    char *token;
    strcpy(tokenizer_copy, create_arguments);
    if (strncmp(tokenizer_copy, "(", 1) == 0) {
        tokenizer_copy++;
        // token stores first argument. Tokenizer copy now points to just past first ,
        token = strsep(&tokenizer_copy, ",");
        if (token == NULL) {
            return INCORRECT_FORMAT;
        } else {
            if (strcmp(token, "db") == 0) {
                mes_status = parse_create_db(tokenizer_copy);
            } else if (strcmp(token, "tbl") == 0) {
                mes_status = parse_create_tbl(tokenizer_copy);
            } else if (strcmp(token, "col") == 0) {
                mes_status = parse_create_col(tokenizer_copy);
            } else if (strcmp(token, "idx") == 0) {
                mes_status = parse_create_idx(tokenizer_copy);
            } else {
                mes_status = UNKNOWN_COMMAND;
            }
        }
    } else {
        mes_status = UNKNOWN_COMMAND;
    }
    free(to_free);
    return mes_status;
}

/**
 * parse_load
 **/

DbOperator* parse_load(char* query_command, message* send_message) {
    size_t i = 0, j = 0;
    unsigned int columns_inserted = 0;
    char* token = NULL;
    if (strncmp(query_command, "(", 1) == 0) {
        query_command++;
        char** command_index = &query_command;
        char* db_tbl_col_name = next_token(command_index, &send_message->status);

        if (send_message->status == INCORRECT_FORMAT) {
            return NULL;
        }

        strsep(&db_tbl_col_name, ".");
        char* tbl_name = strsep(&db_tbl_col_name, ".");
        char* col_name = db_tbl_col_name;
        
        // lookup the table and make sure it exists. 
        Table* table = lookup_table(tbl_name);
        if (table == NULL) {
            send_message->status = OBJECT_NOT_FOUND;
            return NULL;
        }
        size_t numColumns = table->col_count;

        Column* column = NULL;
        //Get the column to fill the data
        for(i=0; i<numColumns; i++){
            column = &(table->columns[i]);
            if(strcmp(column->name, col_name) == 0)
                break;
        }
        

        columns_inserted++;
        size_t dataSize = -1;
        while ((token = strsep(command_index, ",")) != NULL) {
            if(strncmp(token, "load", 4) == 0){
                token += 5;
                db_tbl_col_name = token;
                strsep(&db_tbl_col_name, ".");
                tbl_name = strsep(&db_tbl_col_name, ".");
                col_name = db_tbl_col_name;
                for(j=0; j<numColumns; j++){
                    column = &(table->columns[j]);
                    if(strcmp(column->name, col_name) == 0)
                    {
                        dataSize = -1;
                        columns_inserted++;
                        break;
                    }
                }
            }else{

                int load_val = atoi(token);
                dataSize++;
                //check if the column data size need to be modified.
                if(dataSize == table->col_data_capacity){
                    table->col_data_capacity *= 200; 
                    for(i=0; i<numColumns; i++){
                        Column* resizeColumn = NULL;
                        resizeColumn = &(table->columns[i]);
                        resizeColumn->data = (int*)realloc(resizeColumn->data, (table->col_data_capacity) * sizeof(int));
                    }
                }
                column->data[dataSize] = load_val;
            }
        }
        table->table_length = dataSize+1;

        if (columns_inserted != numColumns) {
            send_message->status = INCORRECT_FORMAT;
            return NULL;
        } 

        DbOperator* dbo = malloc(sizeof(DbOperator));
        dbo->type = CREATE_IDX;
        dbo->operator_fields.create_idx_operator.table = table;
        dbo->operator_fields.create_idx_operator.column = column;
        
        send_message->status = OK_WAIT_FOR_RESPONSE;
        return dbo;
    } else {
        send_message->status = UNKNOWN_COMMAND;
        return NULL;
    }
}

/**
 * parse_insert reads in the arguments for a create statement and 
 * then passes these arguments to a database function to insert a row.
 **/

DbOperator* parse_insert(char* query_command, message* send_message) {
    unsigned int columns_inserted = 0;
    char* token = NULL;
    if (strncmp(query_command, "(", 1) == 0) {
        query_command++;
        char** command_index = &query_command;
        char* db_tbl_name = next_token(command_index, &send_message->status);

        if (send_message->status == INCORRECT_FORMAT) {
            return NULL;
        }

        strsep(&db_tbl_name, ".");
        char* table_name = db_tbl_name;
        
        // lookup the table and make sure it exists. 
        Table* insert_table = lookup_table(table_name);
        if (insert_table == NULL) {
            send_message->status = OBJECT_NOT_FOUND;
            return NULL;
        }
        DbOperator* dbo = malloc(sizeof(DbOperator));
        dbo->type = INSERT;
        dbo->operator_fields.insert_operator.table = insert_table;
        dbo->operator_fields.insert_operator.values = malloc(sizeof(int) * insert_table->col_count);
        while ((token = strsep(command_index, ",")) != NULL) {
            // NOT ERROR CHECKED. COULD WRITE YOUR OWN ATOI. (ATOI RETURNS 0 ON NON-INTEGER STRING)
            int insert_val = atoi(token);
            dbo->operator_fields.insert_operator.values[columns_inserted] = insert_val;
            columns_inserted++;
        }
        // check that we received the correct number of input values
        if (columns_inserted != insert_table->col_count) {
            send_message->status = INCORRECT_FORMAT;
            free (dbo);
            return NULL;
        } 
        send_message->status = OK_WAIT_FOR_RESPONSE;
        return dbo;
    } else {
        send_message->status = UNKNOWN_COMMAND;
        return NULL;
    }
}

static GeneralizedColumn* get_generic_result_column(char* handleName, ClientContext* context, size_t* nData)
{
    int j;
    GeneralizedColumn* pGenericColumn = NULL;
    GeneralizedColumnHandle* pGenHandle = NULL;

    int numCHandles = context->chandles_in_use;
    for(j=0; j<numCHandles; j++){
        pGenHandle = &(context->chandle_table[j]);
        if(strcmp(pGenHandle->name, handleName) == 0 && 
            (pGenHandle->generalized_column.column_type == RESULT)){
            Result* pResult = pGenHandle->generalized_column.column_pointer.result;
            pGenericColumn = malloc(sizeof(GeneralizedColumn));
            pGenericColumn->column_type = RESULT;
            pGenericColumn->column_pointer.result = pResult;
            *nData = pResult->num_tuples;
            break;
        }
    }

    return pGenericColumn;
}

static GeneralizedColumn* get_generic_table_column(char* handleName, size_t* nData, message* send_message)
{
    GeneralizedColumn* pGenericColumn = NULL;

    strsep(&handleName, ".");
    char* tbl_name = strsep(&handleName, ".");
    char* col_name = handleName;
    Table* scan_table = lookup_table(tbl_name);
    if (scan_table == NULL) {
        send_message->status = OBJECT_NOT_FOUND;
        return NULL;
    }
    Column* scan_column = retrieve_column_for_scan(scan_table, col_name, true);
    if (scan_column == NULL) {
        send_message->status = OBJECT_NOT_FOUND;
        return NULL;
    }
    pGenericColumn = malloc(sizeof(GeneralizedColumn));
    pGenericColumn->column_type = COLUMN;
    pGenericColumn->column_pointer.column = scan_column;
    *nData = scan_table->table_length;

    return pGenericColumn;
}
/**
 * parse_max_min
 **/
DbOperator* parse_max_min(char* query_command, char* handle, ClientContext* context, message* send_message, bool isMax){
    if (strncmp(query_command, "(", 1) == 0) {
        query_command++;

        size_t nData = 0;
        GeneralizedColumn* pGenColumn = NULL;
        char** command_index = &query_command;
        char* handle1 = next_token(command_index, &send_message->status);
        char* handle2 = query_command;
        char* pDotChr = strchr(handle1, '.');
        if(handle2 == NULL){
            int last_char = strlen(handle1) - 1;
            if (last_char < 0 || handle1[last_char] != ')') {
                send_message->status = INCORRECT_FORMAT;
                return NULL;
            }
            handle1[last_char] = '\0';
            if(pDotChr == NULL){
                pGenColumn = get_generic_result_column(handle1, context, &nData);
            }else{
                pGenColumn = get_generic_table_column(handle1, &nData, send_message);
            }
            if(pGenColumn == NULL){
                send_message->status = INCORRECT_FORMAT;
                return NULL;
            }

            DbOperator* dbo = malloc(sizeof(DbOperator));
            dbo->type = MAX_MIN;
            dbo->operator_fields.max_min_operator.gen_col = pGenColumn;
            dbo->operator_fields.max_min_operator.num_data = nData;
            dbo->operator_fields.max_min_operator.isMax = isMax;
            dbo->operator_fields.max_min_operator.handle = (char*)malloc(strlen(handle) + 1);
            strcpy(dbo->operator_fields.max_min_operator.handle, handle);

            send_message->status = OK_WAIT_FOR_RESPONSE;
            return dbo;
        }
        

    }else {
        send_message->status = UNKNOWN_COMMAND;
        return NULL;
    }

    return NULL;
}
/**
 * parse_add_sub
 **/
DbOperator* parse_add_sub(char* query_command, char* handle, ClientContext* context, message* send_message, bool isAdd){
    if (strncmp(query_command, "(", 1) == 0) {
        query_command++;
        
        GeneralizedColumn* pGenColumn1 = NULL;
        GeneralizedColumn* pGenColumn2 = NULL;
        size_t nData1 = 0, nData2 = 0;

        char** command_index = &query_command;
        char* handle1 = next_token(command_index, &send_message->status);
        char* handle2 = query_command;
        if(handle1 == NULL || handle2 == NULL){
            send_message->status = INCORRECT_FORMAT;
            return NULL;
        }
        int last_char = strlen(handle2) - 1;
        if (last_char < 0 || handle2[last_char] != ')') {
            send_message->status = INCORRECT_FORMAT;
            return NULL;
        }
        handle2[last_char] = '\0';
        char* pDotChr1 = strchr(handle1, '.');
        if(pDotChr1 == NULL){
            pGenColumn1 = get_generic_result_column(handle1, context, &nData1);
        }else{
            pGenColumn1 = get_generic_table_column(handle1, &nData1, send_message);
        }

        char* pDotChr2 = strchr(handle2, '.');
        if(pDotChr2 == NULL){
            pGenColumn2 = get_generic_result_column(handle2, context, &nData2);
        }else{
            pGenColumn2 = get_generic_table_column(handle2, &nData2, send_message);
        }

        if(pGenColumn1 == NULL || pGenColumn2 == NULL || (nData1 != nData2) )
        {
            send_message->status = INCORRECT_FORMAT;
            return NULL;
        }

        DbOperator* dbo = malloc(sizeof(DbOperator));
        dbo->type = ADD_SUB;
        dbo->operator_fields.add_sub_operator.gen_col1 = pGenColumn1;
        dbo->operator_fields.add_sub_operator.gen_col2 = pGenColumn2;
        dbo->operator_fields.add_sub_operator.num_data = nData1;
        dbo->operator_fields.add_sub_operator.isAdd = isAdd;
        dbo->operator_fields.add_sub_operator.handle = (char*)malloc(strlen(handle) + 1);
        strcpy(dbo->operator_fields.add_sub_operator.handle, handle);

        send_message->status = OK_WAIT_FOR_RESPONSE;
        return dbo;
    }else {
        send_message->status = UNKNOWN_COMMAND;
        return NULL;
    }

    return NULL;
}
/**
 * parse_avg_sum
 **/
DbOperator* parse_avg_sum(char* query_command, char* handle, ClientContext* context, message* send_message, bool isSum){
    if (strncmp(query_command, "(", 1) == 0) {
        query_command++;
        GeneralizedColumn* pGenColumn = NULL;
        size_t nData = 0;
        char* pInputHandle = trim_parenthesis(query_command);
        char* pDotChr = strchr(pInputHandle, '.');
        if(pDotChr == NULL){
            int numCHandles = context->chandles_in_use;
            GeneralizedColumnHandle* pGenHandle = NULL;
            int j;
            for(j=0; j<numCHandles; j++){
                pGenHandle = &(context->chandle_table[j]);
                if(strcmp(pGenHandle->name, pInputHandle) == 0 && 
                    (pGenHandle->generalized_column.column_type == RESULT)){
                    Result* pResult = pGenHandle->generalized_column.column_pointer.result;
                    pGenColumn = malloc(sizeof(GeneralizedColumn));
                    pGenColumn->column_type = RESULT;
                    pGenColumn->column_pointer.result = pResult;
                    nData = pResult->num_tuples;
                    break;
                }
            }
        }
        else{
            strsep(&pInputHandle, ".");
            char* tbl_name = strsep(&pInputHandle, ".");
            char* col_name = pInputHandle;
            Table* scan_table = lookup_table(tbl_name);
            if (scan_table == NULL) {
                send_message->status = OBJECT_NOT_FOUND;
                return NULL;
            }
            Column* scan_column = retrieve_column_for_scan(scan_table, col_name, true);
            if (scan_column == NULL) {
                send_message->status = OBJECT_NOT_FOUND;
                return NULL;
            }
            pGenColumn = malloc(sizeof(GeneralizedColumn));
            pGenColumn->column_type = COLUMN;
            pGenColumn->column_pointer.column = scan_column;
            nData = scan_table->table_length;
        }

        DbOperator* dbo = malloc(sizeof(DbOperator));
        dbo->type = AVG_SUM;
        dbo->operator_fields.avg_sum_operator.gen_col = pGenColumn;
        dbo->operator_fields.avg_sum_operator.num_data = nData;
        dbo->operator_fields.avg_sum_operator.isSum = isSum;
        dbo->operator_fields.avg_sum_operator.handle = (char*)malloc(strlen(handle) + 1);
        strcpy(dbo->operator_fields.avg_sum_operator.handle, handle);
        send_message->status = OK_WAIT_FOR_RESPONSE;

        return dbo;

    }else {
        send_message->status = UNKNOWN_COMMAND;
        return NULL;
    }

    return NULL;
}
/**
 * parse_print
 **/
DbOperator* parse_print(char* query_command, message* send_message){
    char* token = NULL;
    if (strncmp(query_command, "(", 1) == 0) {
        query_command++;
        int index = 0;
        DbOperator* dbo = malloc(sizeof(DbOperator));
        dbo->type = PRINT;
        char** command_index = &query_command;
        while ((token = strsep(command_index, ",")) != NULL) {
            char* pHandle = trim_parenthesis(token);
            dbo->operator_fields.print_operator.handleNames[index] = malloc((strlen(pHandle)+1) * sizeof(char));
            strcpy(dbo->operator_fields.print_operator.handleNames[index], pHandle);
            index++;
        }
        dbo->operator_fields.print_operator.numHandles = index;
        send_message->status = OK_WAIT_FOR_RESPONSE;
        return dbo;
    }else {
        send_message->status = UNKNOWN_COMMAND;
        return NULL;
    }

    return NULL;
}

/**
 * parse_fetch
 **/

DbOperator* parse_fetch(char* query_command, char* handle, message* send_message) {
    if (strncmp(query_command, "(", 1) == 0) {
        query_command++;
        char** command_index = &query_command;
        char* db_tbl_col_name = next_token(command_index, &send_message->status);
        char* targetResultVecName = next_token(command_index, &send_message->status);

        if (send_message->status == INCORRECT_FORMAT) {
            return NULL;
        }

        int last_char = strlen(targetResultVecName) - 1;
        if (targetResultVecName[last_char] != ')') {
            return NULL;
        }
        targetResultVecName[last_char] = '\0';

        strsep(&db_tbl_col_name, ".");
        char* tbl_name = strsep(&db_tbl_col_name, ".");
        char* col_name = db_tbl_col_name;

        Table* scan_table = lookup_table(tbl_name);
        if (scan_table == NULL) {
            send_message->status = OBJECT_NOT_FOUND;
            return NULL;
        }
        Column* scan_column = retrieve_column_for_scan(scan_table, col_name, true);
        if (scan_column == NULL) {
            send_message->status = OBJECT_NOT_FOUND;
            return NULL;
        }
        DbOperator* dbo = malloc(sizeof(DbOperator));
        dbo->type = FETCH;
        dbo->operator_fields.fetch_operator.column = scan_column;
        dbo->operator_fields.fetch_operator.targetVecHandle = (char*)malloc(strlen(targetResultVecName) + 1);
        strcpy(dbo->operator_fields.fetch_operator.targetVecHandle, targetResultVecName);
        dbo->operator_fields.fetch_operator.handle = (char*)malloc(strlen(handle) + 1);
        strcpy(dbo->operator_fields.fetch_operator.handle, handle);

        send_message->status = OK_WAIT_FOR_RESPONSE;
        return dbo;

    }else {
        send_message->status = UNKNOWN_COMMAND;
        return NULL;
    }

    return NULL;
}
void copyResultGenericColumn(GeneralizedColumn* srcGenColumn, GeneralizedColumn* dstGenColumn)
{
    dstGenColumn->column_type = srcGenColumn->column_type;
    dstGenColumn->column_pointer.result = srcGenColumn->column_pointer.result;
}

void copyTblColGenericColumn(GeneralizedColumn* srcGenColumn, GeneralizedColumn* dstGenColumn)
{
    dstGenColumn->column_type = srcGenColumn->column_type;
    dstGenColumn->column_pointer.column = srcGenColumn->column_pointer.column;
}
/**
 * parse_select 
 **/
DbOperator* parse_select(char* query_command, char* handle, ClientContext* context, message* send_message) {
    if (strncmp(query_command, "(", 1) == 0) {
        query_command++;
        char** command_index = &query_command;
        char* db_tbl_col_name = next_token(command_index, &send_message->status);
        char *tempToFree, *handle1;
        tempToFree = handle1 = (char*) malloc(strlen(db_tbl_col_name) + 1);
        strcpy(handle1, db_tbl_col_name);
    
        if (send_message->status == INCORRECT_FORMAT) {
            return NULL;
        }

        strsep(&db_tbl_col_name, ".");
        GeneralizedColumn* pGenColumn = NULL;
        size_t numGenericColumns = 0;
        Table* scan_table = NULL;
        //SELECT - type 2.
        if(db_tbl_col_name == NULL){
            char* handle2 = next_token(command_index, &send_message->status);
            numGenericColumns = 2;
            pGenColumn = malloc(sizeof(GeneralizedColumn) * numGenericColumns);
            memset(pGenColumn, 0, sizeof(GeneralizedColumn) * numGenericColumns);
            size_t nData1 = 0, nData2 = 0;
            GeneralizedColumn* tempGenColumn = NULL;
            tempGenColumn = get_generic_result_column(handle1, context, &nData1);
            copyResultGenericColumn(tempGenColumn, &pGenColumn[0]);
            free(tempGenColumn); tempGenColumn = NULL;
            char* pDotChr2 = strchr(handle2, '.');
            if(pDotChr2 == NULL){
                tempGenColumn = get_generic_result_column(handle2, context, &nData2);
                copyResultGenericColumn(tempGenColumn, &pGenColumn[1]);
                free(tempGenColumn); tempGenColumn = NULL;
            }else{
                tempGenColumn = get_generic_table_column(handle2, &nData2, send_message);
                copyTblColGenericColumn(tempGenColumn, &pGenColumn[1]);
                free(tempGenColumn); tempGenColumn = NULL;
            }

        }else{
            numGenericColumns = 1;
            strsep(&handle1, ".");
            char* tbl_name = strsep(&handle1, ".");
            char* col_name = handle1;
            scan_table = lookup_table(tbl_name);
            if (scan_table == NULL) {
                send_message->status = OBJECT_NOT_FOUND;
                return NULL;
            }
            Column* scan_column = retrieve_column_for_scan(scan_table, col_name, true);
            if (scan_column == NULL) {
                send_message->status = OBJECT_NOT_FOUND;
                return NULL;
            }
            pGenColumn = malloc(sizeof(GeneralizedColumn));
            pGenColumn->column_type = COLUMN;
            pGenColumn->column_pointer.column = scan_column;
        }

        char* low_val = next_token(command_index, &send_message->status);
        char* high_val = next_token(command_index, &send_message->status);
        int last_char = strlen(high_val) - 1;
        if (high_val[last_char] != ')') {
            send_message->status = INCORRECT_FORMAT;
            return NULL;
        }
        high_val[last_char] = '\0';
        
        DbOperator* dbo = malloc(sizeof(DbOperator));
        dbo->type = SELECT;
        dbo->operator_fields.select_operator.table = scan_table;

        Comparator* comparator = malloc(sizeof(Comparator));
        memset(comparator, 0, sizeof(Comparator));
        if(strcmp(low_val, "null") == 0){
            comparator->type1 = NO_COMPARISON;
        }
        else{
            comparator->type1 = GREATER_THAN_OR_EQUAL;
            int low_value = atoi(low_val);
            comparator->p_low = low_value;
        }
        if(strcmp(high_val, "null") == 0){
            comparator->type2 = NO_COMPARISON;
        }
        else{
            comparator->type2 = LESS_THAN;
            int high_value = atoi(high_val);
            comparator->p_high = high_value;
        }

        comparator->gen_col = pGenColumn;
        comparator->numInputCols = numGenericColumns;
        comparator->handle = (char*)malloc(strlen(handle) + 1);
        strcpy(comparator->handle, handle);

        dbo->operator_fields.select_operator.comparator = comparator;
        free(tempToFree);

        send_message->status = OK_WAIT_FOR_RESPONSE;
        return dbo;
        
    }else {
        send_message->status = UNKNOWN_COMMAND;
        return NULL;
    }

    return NULL;
}

/**
 * parse_command takes as input the send_message from the client and then
 * parses it into the appropriate query. Stores into send_message the
 * status to send back.
 * Returns a db_operator.
 **/
DbOperator* parse_command(char* query_command, message* send_message, int client_socket, ClientContext* context) {
    DbOperator *dbo = NULL; // = malloc(sizeof(DbOperator)); // calloc?

    if (strncmp(query_command, "--", 2) == 0) {
        send_message->status = OK_WAIT_FOR_RESPONSE;
        // COMMENT LINE! 
        return NULL;
    }

    char *equals_pointer = strchr(query_command, '=');
    char *handle = query_command;
    if (equals_pointer != NULL) {
        // handle file table
        *equals_pointer = '\0';
        cs165_log(stdout, "FILE HANDLE: %s\n", handle);
        query_command = ++equals_pointer;
    } else {
        handle = NULL;
    }

    //cs165_log(stdout, "QUERY: %s\n", query_command);

    send_message->status = OK_WAIT_FOR_RESPONSE;
    query_command = trim_whitespace(query_command);

    if (strncmp(query_command, "create", 6) == 0) {
        query_command += 6;
        send_message->status = parse_create(query_command);
        dbo = malloc(sizeof(DbOperator));
        dbo->type = CREATE;
    } else if (strncmp(query_command, "relational_insert", 17) == 0) {
        query_command += 17;
        dbo = parse_insert(query_command, send_message);

    }else if (strncmp(query_command, "load", 4) == 0) {
        query_command += 4;
        dbo = parse_load(query_command, send_message);

    }
    else if (strncmp(query_command, "select", 6) == 0){
        query_command += 6;
        dbo = parse_select(query_command, handle, context, send_message);

    }else if(strncmp(query_command, "fetch", 5) == 0){
        query_command += 5;
        dbo = parse_fetch(query_command, handle, send_message);

    }else if(strncmp(query_command, "print", 5) == 0){
        query_command += 5;
        dbo = parse_print(query_command, send_message);

    }else if(strncmp(query_command, "avg", 3) == 0){
        query_command += 3;
        dbo = parse_avg_sum(query_command, handle, context, send_message, false);

    }else if(strncmp(query_command, "sum", 3) == 0){
        query_command += 3;
        dbo = parse_avg_sum(query_command, handle, context, send_message, true);

    }else if(strncmp(query_command, "add", 3) == 0){
        query_command += 3;
        dbo = parse_add_sub(query_command, handle, context, send_message, true);

    }else if(strncmp(query_command, "sub", 3) == 0){
        query_command += 3;
        dbo = parse_add_sub(query_command, handle, context, send_message, false);

    }else if(strncmp(query_command, "max", 3) == 0){
        query_command += 3;
        dbo = parse_max_min(query_command, handle, context, send_message, true);

    }else if(strncmp(query_command, "min", 3) == 0){
        query_command += 3;
        dbo = parse_max_min(query_command, handle, context, send_message, false);

    }else if (strncmp(query_command, "shutdown", 8) == 0) {
        Status ret_status;
        ret_status = saveDatabase();
        shutdown_server();
        if(ret_status.code != ERROR)
            send_message->status = SHUTDOWN_REQUESTED;
        else
            send_message->status = EXECUTION_ERROR;
    }
    if (dbo == NULL) {
        return dbo;
    }
    
    dbo->client_fd = client_socket;
    dbo->context = context;
    return dbo;
}
