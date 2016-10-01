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
        cs165_log(stdout, "command_index: %s \n", *command_index);

        if (send_message->status == INCORRECT_FORMAT) {
            return NULL;
        }

        strsep(&db_tbl_col_name, ".");
        char* tbl_name = strsep(&db_tbl_col_name, ".");
        char* col_name = db_tbl_col_name;

        Table* scan_table = lookup_table(tbl_name);
        if (scan_table == NULL) {
            send_message->status = OBJECT_NOT_FOUND;
            return NULL;
        }
        Column* scan_column = retrieve_column_for_scan(scan_table, col_name);
        if (scan_column == NULL) {
            send_message->status = OBJECT_NOT_FOUND;
            return NULL;
        }
        DbOperator* dbo = malloc(sizeof(DbOperator));
        dbo->type = FETCH;
        dbo->operator_fields.fetch_operator.column = scan_column;
        strcpy(dbo->operator_fields.fetch_operator.handle, handle);

        send_message->status = OK_WAIT_FOR_RESPONSE;
        return dbo;

    }else {
        send_message->status = UNKNOWN_COMMAND;
        return NULL;
    }

    return NULL;
}
/**
 * parse_select 
 **/

DbOperator* parse_select(char* query_command, char* handle, message* send_message) {
    if (strncmp(query_command, "(", 1) == 0) {
        query_command++;
        char** command_index = &query_command;
        char* db_tbl_col_name = next_token(command_index, &send_message->status);
        cs165_log(stdout, "command_index: %s \n", *command_index);

        if (send_message->status == INCORRECT_FORMAT) {
            return NULL;
        }

        strsep(&db_tbl_col_name, ".");

        //SELECT - type 2.
        if(db_tbl_col_name == NULL){

        }else{
            char* tbl_name = strsep(&db_tbl_col_name, ".");
            char* col_name = db_tbl_col_name;

            Table* scan_table = lookup_table(tbl_name);
            if (scan_table == NULL) {
                send_message->status = OBJECT_NOT_FOUND;
                return NULL;
            }

            Column* scan_column = retrieve_column_for_scan(scan_table, col_name);
            if (scan_column == NULL) {
                send_message->status = OBJECT_NOT_FOUND;
                return NULL;
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

            GeneralizedColumn* pGenColumn = malloc(sizeof(GeneralizedColumn));
            pGenColumn->column_type = COLUMN;
            pGenColumn->column_pointer.column = scan_column;
            comparator->gen_col = pGenColumn;
            strcpy(comparator->handle, handle);

            dbo->operator_fields.select_operator.comparator = comparator;

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

    cs165_log(stdout, "QUERY: %s\n", query_command);

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

    }else if (strncmp(query_command, "select", 6) == 0){
        query_command += 6;
        dbo = parse_select(query_command, handle, send_message);

    }else if(strncmp(query_command, "fetch", 5) == 0){
        query_command += 5;
        dbo = parse_fetch(query_command, handle, send_message);

    }else if(strncmp(query_command, "print", 5) == 0){
        query_command += 5;
        dbo = parse_print(query_command, send_message);

    }else if (strncmp(query_command, "shutdown", 8) == 0) {
        Status ret_status;
        ret_status = saveDatabase();
        if(ret_status.code != ERROR)
            send_message->status = OK_DONE;
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
