#include "cs165_api.h"
#include <string.h>

// In this class, there will always be only one active database at a time
Db *current_db;

Table* create_table(Db* db, const char* name, size_t num_columns, Status *ret_status) {
	if(current_db->tables_size < current_db->tables_capacity){
		Table* table = (Table*)malloc(sizeof(Table));
		(current_db->tables_size)++;
		current_db->tables = (Table*)realloc(current_db->tables, current_db->tables_size);
		strcpy(table->name, name);
		table->num_columns = num_columns;
		table->columns = NULL;
		table->col_count = 0;
	}
	ret_status->code=OK;
	return NULL;
}

Status add_db(const char* db_name, bool new) {
	struct Status ret_status;
	if(new){
		current_db = (Db*)malloc(sizeof(Db));
		strcpy(current_db->name, db_name);
		current_db->tables_size = 0;
		current_db->tables_capacity = 100;  //For now, num of max tables are 100
		current_db->tables = NULL;
	}
	
	ret_status.code = OK;
	return ret_status;
}
