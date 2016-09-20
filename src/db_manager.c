#include "cs165_api.h"
#include <string.h>

// In this class, there will always be only one active database at a time
Db *current_db;

Column* create_column(char *name, Table *table, bool sorted, Status *ret_status){
	Column* column = NULL;
	int i=0;
	for(i=0; i<(table->col_count); i++){
		if(strcmp(table->columns[i].name, "") == 0){
			strcpy(table->columns[i].name, name);
			column = &(table->columns[i]);
			break;
		}
	}
	ret_status->code=OK;

	return column;
}

Table* create_table(Db* db, const char* name, size_t num_columns, Status *ret_status) {
	Table* table = NULL;
	if(db->tables_size < db->tables_capacity){
		table = (Table*)malloc(sizeof(Table));
		(db->tables_size)++;
		db->tables = (Table*)realloc(db->tables, db->tables_size);
		strcpy(table->name, name);
		table->columns = (Column*)malloc(sizeof(Column)*num_columns);
		int i=0;
		for(i=0; i<num_columns; i++){
			strcpy(table->columns[i].name, "");
		}
		table->col_count = num_columns;
		table->table_length = 0;
		ret_status->code=OK;
	}
	
	return table;
}

void add_db(const char* db_name, bool new, Status* status) {
	if(new){
		if(current_db != NULL){
			free(current_db);
			current_db = NULL;
		}

		current_db = (Db*)malloc(sizeof(Db));
		strcpy(current_db->name, db_name);
		current_db->tables_size = 0;
		current_db->tables_capacity = 100;  //For now, num of max tables are 100
		current_db->tables = NULL;
		status->code = OK;
	}
}
