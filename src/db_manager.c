#include "cs165_api.h"

#include <sys/types.h>
#include <dirent.h>

#include <string.h>
#include <sys/stat.h>
#include <errno.h>
#include <stdbool.h>

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
	int i=0;
	
	if(db->tables_size < db->tables_capacity){
		//table = (Table*)malloc(sizeof(Table));
		(db->tables_size)++;
		db->tables = (Table*)realloc(db->tables, db->tables_size);
		int tableIndex = (db->tables_size)-1;
		Table* table = &(db->tables[tableIndex]);
		strcpy(table->name, name);
		table->columns = (Column*)malloc(sizeof(Column)*num_columns);
		for(i=0; i<num_columns; i++){
			strcpy(table->columns[i].name, "");
		}
		table->col_count = num_columns;
		table->table_length = 0;
		
		ret_status->code=OK;

		//Check if the directory for table exists in the database.
		char tablePath[128];
		strcpy(tablePath, "../database/");
		strcat(tablePath, db->name);
		strcat(tablePath, "/");
		strcat(tablePath, name);
		DIR* dir = opendir(tablePath);
		if (dir){
		    closedir(dir);
		}
		else if (ENOENT == errno){
			int status;
			status = mkdir(tablePath, S_IRWXU | S_IRWXG | S_IROTH | S_IXOTH);
			if(status == -1){
				ret_status->code = ERROR;
			}
		}
	}
	
	return &(db->tables[(db->tables_size)-1]);
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

		char dbPath[128];
		strcpy(dbPath, "../database/");
		strcat(dbPath, db_name);
		DIR* dir = opendir(dbPath);
		if (dir){
		    closedir(dir);
		}
		else if (ENOENT == errno){
			int retStatus;
			retStatus = mkdir(dbPath, S_IRWXU | S_IRWXG | S_IROTH | S_IXOTH);
			if(retStatus == -1){
				status->code = ERROR;
			}
		}
		status->code = OK;
	}
}

Status db_startup(){
	Status ret_status;
	ret_status.code = OK;
	bool isNewDir = false;
	Catalog catalog;
	int i, j;
	memset(&(catalog.dbName), '\0', MAX_SIZE_NAME);


	//Check the existence of "database" directory.
	DIR* dir = opendir("../database");
	if (dir){
	    closedir(dir);
	}
	else if (ENOENT == errno){
		int status;
		status = mkdir("../database", S_IRWXU | S_IRWXG | S_IROTH | S_IXOTH);
		if(status == -1){
			ret_status.code = ERROR;
		}
		isNewDir = true;
	}
	else{
	    ret_status.code = ERROR;
	}


	if(ret_status.code ==  ERROR || isNewDir)
		return ret_status;

	//If database folder just created for the first time, 
	//i.e. database catalog is non-existent. 
	//The catalog would be created at the time of shutdown.
	FILE *ptr_catalog;
	ptr_catalog=fopen("../database/catalog.bin","r+b");

	if(!ptr_catalog){
		ret_status.code ==  ERROR;
		return ret_status;
	}

	size_t rCount = fread(&catalog,sizeof(Catalog),1,ptr_catalog);

	//read from the catalog and initialized local data
	current_db = (Db*)malloc(sizeof(Db));
	strcpy(current_db->name, catalog.dbName);
	current_db->tables_size = catalog.numTables;
	current_db->tables_capacity = catalog.numTableCapacity;  //For now, num of max tables are 100
	current_db->tables = (Table*)malloc(sizeof(Table)*(catalog.numTables));
	for(i=0; i<(current_db->tables_size); i++){
		//Table name
		strcpy((current_db->tables[i]).name, catalog.tableNames[i]);
		//Number of Columns
		int numTabColumns = catalog.numTableColumns[i];
		current_db->tables[i].col_count = numTabColumns;
		//Column size
		current_db->tables[i].table_length = catalog.columnSize[i];
		(current_db->tables[i]).columns = (Column*)malloc(sizeof(Column)*numTabColumns);
		for(j=0; j<numTabColumns; j++){
			strcpy((current_db->tables[i]).columns[j].name, catalog.columnNames[i][j]);
		}
	}
	fclose(ptr_catalog);
	return ret_status;
}

Status saveDatabse(){
	Status ret_status;
	Catalog catalog;
	int i, j;
	ret_status.code = OK;

	FILE *ptr_catalog;
	ptr_catalog=fopen("../database/catalog.bin","wb");

	if(!ptr_catalog){
		ret_status.code ==  ERROR;
		return ret_status;
	}

	strcpy(catalog.dbName, current_db->name);
	catalog.numTables = current_db->tables_size;
	catalog.numTableCapacity = current_db->tables_capacity;  
	for(i=0; i<(current_db->tables_size); i++){
		char* tableName = current_db->tables[i].name;
		strcpy(catalog.tableNames[i], tableName);
		int numTableColumns = current_db->tables[i].col_count;
		catalog.numTableColumns[i] = numTableColumns;
		int tableLength = current_db->tables[i].table_length;
		catalog.columnSize[i] = tableLength;

		char tablePath[256];
		strcpy(tablePath, "../database/");
		strcat(tablePath, current_db->name);
		strcat(tablePath, "/");
		strcat(tablePath, tableName);
		strcat(tablePath, "/");

		for(j=0; j<numTableColumns; j++){
			Column column = (current_db->tables[i]).columns[j];
			char* colName = column.name;
			strcpy(catalog.columnNames[i][j], colName);
			FILE *ptr_column;

			char colPath[256];
			strcpy(colPath, tablePath);
			strcat(colPath, colName);
			ptr_column=fopen(colPath, "wb");

			if(!ptr_column){
				ret_status.code ==  ERROR;
				return ret_status;
			}

			fwrite(column.data, sizeof(int), tableLength, ptr_column);
			fclose(ptr_column);
		}
	}

	//Write the data into the catalog file
	fwrite(&catalog, sizeof(Catalog), 1, ptr_catalog);

	//Free up the memory of data strcutures
	for(i=0; i<(current_db->tables_size); i++){
		Column* cols = current_db->tables[i].columns;
		int numTableColumns = current_db->tables[i].col_count;
		for(j=0; j<numTableColumns; j++){
			int* colData = (current_db->tables[i]).columns[j].data;
			if(colData != NULL)
				free(colData);
		}
		free(cols);
	}
	free(current_db->tables);
	free(current_db);

	fclose(ptr_catalog);
	return ret_status;
}