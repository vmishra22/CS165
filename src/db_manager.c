#include "cs165_api.h"

#include <sys/types.h>
#include <dirent.h>

#include <string.h>
#include <sys/stat.h>
#include <errno.h>
#include <stdbool.h>
#include "btree.h"

// In this class, there will always be only one active database at a time
Db *current_db;

Column* create_column(char *name, Table *table, bool sorted, Status *ret_status){
	(void) sorted;
	Column* column = NULL;
	size_t i=0;
	size_t numColumns = table->col_count;
	for(i=0; i<numColumns; i++){
		if(strcmp(table->columns[i].name, "") == 0){
			strcpy(table->columns[i].name, name);
			column = &(table->columns[i]);
			column->data = (int*)malloc(sizeof(int) * (table->col_data_capacity));
			column->index = NULL;
			break;
		}
	}
	ret_status->code=OK;

	return column;
}

Table* create_table(Db* db, const char* name, size_t num_columns, Status *ret_status) {
	size_t i=0;
	
	if(db->tables_size == db->tables_capacity){
		db->tables_capacity *= 2; 
		db->tables = (Table*)realloc(db->tables, (db->tables_capacity) * sizeof(Table));
	}
	(db->tables_size)++;
	int tableIndex = (db->tables_size)-1;
	Table* table = &(db->tables[tableIndex]);
	strcpy(table->name, name);
	table->columns = (Column*)malloc(sizeof(Column)*num_columns);
	for(i=0; i<num_columns; i++){
		strcpy(table->columns[i].name, "");
	}
	table->col_count = num_columns;
	table->col_data_capacity = 1100;
	table->table_length = 0;
	strcpy(table->firstDeclaredClustCol, "");
	
	ret_status->code=OK;

	//Check if the directory for table exists in the database.
	char tablePath[128];
	strcpy(tablePath, "../data/");
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
		current_db->tables_capacity = 10;  //For now, num of max tables are 10
		current_db->tables = (Table*)malloc((current_db->tables_capacity) * sizeof(Table));
		current_db->tables_size = 0;

		char dbPath[128];
		strcpy(dbPath, "../data/");
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
	size_t i;
	int j;
	memset(&(catalog.dbName), '\0', MAX_SIZE_NAME);


	//Check the existence of "data" directory.
	DIR* dir = opendir("../data");
	if (dir){
	    closedir(dir);
	}
	else if (ENOENT == errno){
		int status;
		status = mkdir("../data", S_IRWXU | S_IRWXG | S_IROTH | S_IXOTH);
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
	ptr_catalog=fopen("../data/catalog.bin","r+b");

	if(!ptr_catalog){
		ret_status.code = ERROR;
		return ret_status;
	}

	size_t readVals = fread(&catalog,sizeof(Catalog),1,ptr_catalog);
	(void) readVals;

	//read from the catalog and initialized local data
	current_db = (Db*)malloc(sizeof(Db));
	strcpy(current_db->name, catalog.dbName);
	current_db->tables_size = catalog.numTables;
	current_db->tables_capacity = catalog.numTableCapacity;  //For now, num of max tables are 100
	current_db->tables = (Table*)malloc(sizeof(Table)*(catalog.numTableCapacity));
	for(i=0; i<(current_db->tables_size); i++){
		Table* table = &(current_db->tables[i]);
		//Table name
		strcpy(table->name, catalog.tableNames[i]);
		//Number of Columns
		int numTabColumns = catalog.numTableColumns[i];
		table->col_count = numTabColumns;
		//Column size
		table->table_length = catalog.columnSize[i];
		table->col_data_capacity = catalog.columnDataCapacity[i];
		table->columns = (Column*)malloc(sizeof(Column)*numTabColumns);
		memset(table->columns, 0, sizeof(Column)*numTabColumns);
        strcpy(table->firstDeclaredClustCol, catalog.firstDeclaredClustCol[i]);
		for(j=0; j<numTabColumns; j++){
			Column* column = &(table->columns[j]);
			strcpy(column->name, catalog.columnNames[i][j]);
			column->data = NULL;
		}

		//Check if any column has index
		bool isIndexExisting = false;
		for(j=0; j<numTabColumns; j++){
			if( strcmp(catalog.columnIndexClustType[i][j], "clustered") == 0 ||
				strcmp(catalog.columnIndexClustType[i][j], "unclustered") == 0){
				isIndexExisting = true;
				break;
			}
		}
		if(isIndexExisting){
			for(j=0; j<numTabColumns; j++){
				Column* column = &(table->columns[j]);
				char* colName = column->name;
				column->index = (ColumnIndex*)malloc(sizeof(ColumnIndex));
	            memset(column->index, 0, sizeof(ColumnIndex));
	            ColumnIndex* pIndex = column->index;
	            pIndex->index_data_capacity = catalog.indexDataCapacity[i];
	            pIndex->tuples = (dataRecord*)malloc(sizeof(dataRecord)*(pIndex->index_data_capacity));
	            memset(pIndex->tuples, 0, sizeof(dataRecord)*(pIndex->index_data_capacity));
	            if( strcmp(catalog.columnIndexClustType[i][j], "clustered") == 0)
	            	pIndex->clustered = true;
	            else if(strcmp(catalog.columnIndexClustType[i][j], "unclustered") == 0)
	            	pIndex->unclustered = true;
	            else{
	            	pIndex->clustered = false; pIndex->unclustered = false;
	            }

	            FILE *ptr_index;
				char indexName[64];
				strcpy(indexName, colName);
				strcat(indexName, "Index");
				char indexPath[256];
				strcpy(indexPath, "../data/");
				strcat(indexPath, current_db->name);
				strcat(indexPath, "/");
				strcat(indexPath, table->name);
				strcat(indexPath, "/");
				strcat(indexPath, indexName);
				ptr_index=fopen(indexPath, "rb");

				if(!ptr_index){
					ret_status.code =  ERROR;
					return ret_status;
				}

	            if(strcmp(catalog.columnIndexType[i][j], "SORTED") == 0){
	            	pIndex->indexType = SORTED;
					size_t readVal = fread(pIndex->tuples, sizeof(dataRecord), table->table_length, ptr_index);
					(void)readVal;
	            }
	            else if(strcmp(catalog.columnIndexType[i][j], "BTREE") == 0){
	            	pIndex->indexType = BTREE;
	            	treeRoot* root = NULL;
	            	root = read_tree_from_file(ptr_index);
	            	pIndex->dataIndex = root;
	            	getTreeDataRecords(root, &(pIndex->tuples));
	            }
	            else{
	            	size_t readVal = fread(pIndex->tuples, sizeof(dataRecord), table->table_length, ptr_index);
	            	(void)readVal;
	            }
	            fclose(ptr_index);
        	}
		}
	}
	fclose(ptr_catalog);
	return ret_status;
}

Status saveDatabase(){
	Status ret_status;
	Catalog catalog;
	size_t i;
	int j;
	ret_status.code = OK;

	memset(&catalog, 0, sizeof(Catalog));

	FILE *ptr_catalog;
	ptr_catalog=fopen("../data/catalog.bin","wb");

	if(!ptr_catalog){
		ret_status.code =  ERROR;
		return ret_status;
	}

	strcpy(catalog.dbName, current_db->name);
	catalog.numTables = current_db->tables_size;
	catalog.numTableCapacity = current_db->tables_capacity;  
	for(i=0; i<(current_db->tables_size); i++){
		Table* table = &(current_db->tables[i]);
		char* tableName = table->name;
		strcpy(catalog.tableNames[i], tableName);
		int numTableColumns = table->col_count;
		catalog.numTableColumns[i] = numTableColumns;
		int tableLength = table->table_length;
		catalog.columnSize[i] = tableLength;
		catalog.columnDataCapacity[i] = table->col_data_capacity;
		if(strcmp(table->firstDeclaredClustCol, "") != 0)
            strcpy(catalog.firstDeclaredClustCol[i], table->firstDeclaredClustCol);

		char tablePath[256];
		strcpy(tablePath, "../data/");
		strcat(tablePath, current_db->name);
		strcat(tablePath, "/");
		strcat(tablePath, tableName);
		strcat(tablePath, "/");

		for(j=0; j<numTableColumns; j++){
			Column* column = &(table->columns[j]);
			char* colName = column->name;
			strcpy(catalog.columnNames[i][j], colName);
			//If nothing to serialize then keep iterating..
			if(column->data == NULL)
				continue;
			
			FILE *ptr_column;

			char colPath[256];
			strcpy(colPath, tablePath);
			strcat(colPath, colName);
			ptr_column=fopen(colPath, "wb");

			if(!ptr_column){
				ret_status.code =  ERROR;
				return ret_status;
			}

			fwrite(column->data, sizeof(int), tableLength, ptr_column);
			fclose(ptr_column);
		}

		for(j=0; j<numTableColumns; j++){
			Column* column = &(table->columns[j]);
			char* colName = column->name;
			ColumnIndex* pIndex = NULL;
			pIndex = column->index;
			if(pIndex != NULL){
				FILE *ptr_index;
				char indexName[64];
				strcpy(indexName, colName);
				strcat(indexName, "Index");
				char indexPath[256];
				strcpy(indexPath, tablePath);
				strcat(indexPath, indexName);
				ptr_index=fopen(indexPath, "wb");

				if(!ptr_index){
					ret_status.code =  ERROR;
					return ret_status;
				}

				if(pIndex->clustered)
					strcpy(catalog.columnIndexClustType[i][j], "clustered");
				else if(pIndex->unclustered)
					strcpy(catalog.columnIndexClustType[i][j], "unclustered");
				else
					strcpy(catalog.columnIndexClustType[i][j], "");

				catalog.indexDataCapacity[i] = pIndex->index_data_capacity;

				if(pIndex->indexType == SORTED){
					strcpy(catalog.columnIndexType[i][j], "SORTED");
					dataRecord* colTuplesArr = pIndex->tuples;
					fwrite(colTuplesArr, sizeof(dataRecord), tableLength, ptr_index);

				}else if(pIndex->indexType == BTREE){
					strcpy(catalog.columnIndexType[i][j], "BTREE");
					write_tree_to_file(pIndex->dataIndex, ptr_index);
				}
				else{
					strcpy(catalog.columnIndexType[i][j], "");
					dataRecord* colTuplesArr = pIndex->tuples;
					fwrite(colTuplesArr, sizeof(dataRecord), tableLength, ptr_index);
				}
				fclose(ptr_index);
			}
		}
	}

	//Write the data into the catalog file
	fwrite(&catalog, sizeof(Catalog), 1, ptr_catalog);

	fclose(ptr_catalog);
	return ret_status;
}

Status shutdown_server(){
	Status ret_status;
	ret_status.code = OK;

	size_t i, j;
	//Free up the memory of data strcutures
	for(i=0; i<(current_db->tables_size); i++){
		Column* cols = current_db->tables[i].columns;
		size_t numTableColumns = current_db->tables[i].col_count;
		for(j=0; j<numTableColumns; j++){
			int* colData = (current_db->tables[i]).columns[j].data;
			if(colData != NULL)
				free(colData);
			ColumnIndex* index = NULL;
			index = (current_db->tables[i]).columns[j].index;
			if(index != NULL){
				if(index->tuples != NULL)
					free(index->tuples);
				if(index->indexType == BTREE){
					treeRoot* root = index->dataIndex;
					if(root != NULL)
						destroy_tree(root);
				}
				free(index);
			}
		}
		free(cols);
	}
	free(current_db->tables);
	free(current_db);

	return ret_status;
}