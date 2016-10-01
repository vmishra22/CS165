
#include "client_context.h"
#include <string.h>

Table* lookup_table(char *name) {
	size_t i;
	for(i=0; i<(current_db->tables_size); i++){
		if(strcmp(current_db->tables[i].name, name) == 0){
			return &(current_db->tables[i]);
		}
	}
	    
	return NULL;
}

Column* retrieve_column_for_scan(Table* table, char* colname){
	size_t i;
	size_t numColumns = table->col_count;
	for(i=0; i<numColumns; i++){
		if(strcmp(table->columns[i].name, colname) == 0){
			Column* scanColumn = &(table->columns[i]);
			char colPath[256];
			strcpy(colPath, "../data/");
			strcat(colPath, current_db->name);
			strcat(colPath, "/");
			strcat(colPath, table->name);
			strcat(colPath, "/");
			strcat(colPath, colname);

			FILE *ptr_column;
			ptr_column=fopen(colPath, "rb");
			if(!ptr_column){
				return NULL;
			}
			scanColumn->data = (int*)malloc(sizeof(int)*(table->table_length));
			size_t read_val = fread(scanColumn->data, sizeof(int), table->table_length, ptr_column);
			(void)read_val;
			fclose(ptr_column);
			return scanColumn;
		}
	}

	return NULL;
}

static Result* computeResultIndices(Table* table, Column* column, Comparator* comparator){
	size_t i=0, j=0;
	size_t columnSize = table->table_length;
	int *resultIndices = (int*)malloc(sizeof(int) * columnSize);
	memset(resultIndices, -1, sizeof(int)*columnSize);
	Result* pResult = (Result*)malloc(sizeof(Result));
	memset(pResult, 0, sizeof(Result));
	
	if(comparator->type1 == NO_COMPARISON && comparator->type2 == NO_COMPARISON){
		for (i=0; i<columnSize; i++){
			resultIndices[j++]=i;
		}
	}else if(comparator->type1 == NO_COMPARISON && comparator->type2 == LESS_THAN){
		for (i=0; i<columnSize; i++){
			if(column->data[i] < comparator->p_high)
				resultIndices[j++]=i;
		}
	}else if(comparator->type1 == GREATER_THAN_OR_EQUAL && comparator->type2 == NO_COMPARISON){
		for (i=0; i<columnSize; i++){
			if(column->data[i] >= comparator->p_low)
				resultIndices[j++]=i;
		}
	}else if(comparator->type1 == GREATER_THAN_OR_EQUAL && comparator->type2 == LESS_THAN){
		for (i=0; i<columnSize; i++){
			if(column->data[i] >= comparator->p_low && column->data[i] < comparator->p_high)
				resultIndices[j++]=i;
		}
	}
	
	pResult->payload = resultIndices;
	pResult->num_tuples = j;
	pResult->data_type = INT;
	return pResult;
}

/** execute_DbOperator takes as input the DbOperator and executes the query.
 **/
void execute_DbOperator(DbOperator* query, char** msg) {
    
    char defaultMsg[] = "Blank Operation Succeeded\n";
    strcpy(*msg, defaultMsg);
    if(!query)
        return;

    size_t i;
    int j;
    ClientContext* context = query->context;
    Comparator* comparator = NULL;
    Table* table = NULL;
    Column* column = NULL;
    switch(query->type){
        case INSERT: 
            table = query->operator_fields.insert_operator.table;
            size_t numColumns = table->col_count;

            size_t columnSize = table->table_length;
            //check if the column data size need to be modified.
            if(columnSize == table->col_data_capacity){
            	table->col_data_capacity *= 2; 
            	for(i=0; i<numColumns; i++){
            		column = &(table->columns[i]);
            		column->data = (int*)realloc(column->data, (table->col_data_capacity) * sizeof(int));
            	}
            }
            
            //Copy the values into column data.
            for(i=0; i<numColumns; i++){
                column = &(table->columns[i]);
                column->data[columnSize] = query->operator_fields.insert_operator.values[i];
            }
            (table->table_length)++;
            char insertMsg[] = "Insert Operation Succeeded";
            char str[128];
            memset(str, '\0', 128);
            sprintf(str, "%s Column Size: %zu, First Col value: %d\n",
                     insertMsg, columnSize, query->operator_fields.insert_operator.values[0]);
            strcpy(*msg, str);
           
            break;
        case CREATE:
            break;
        case OPEN:
            break;
        case SELECT:
        	
        	comparator = query->operator_fields.select_operator.comparator;
        	table = query->operator_fields.select_operator.table;
        	GeneralizedColumn* pGenColumn = comparator->gen_col;
        	if( pGenColumn->column_type == COLUMN){
        		column = pGenColumn->column_pointer.column;
        		if(column == NULL){
        			strcpy(*msg, "Column not found to scan");
        			break;
        		}
        		Result* resultIndices = computeResultIndices(table, column, comparator);
        		if(context->chandles_in_use == context->chandle_slots){
		            context->chandle_slots *= 2; 
		            context->chandle_table = (GeneralizedColumnHandle*)
		                            realloc(context->chandle_table, (context->chandle_slots) * sizeof(GeneralizedColumnHandle));
        		}
        		GeneralizedColumnHandle* pGenHandle = &(context->chandle_table[context->chandles_in_use]);
        		strcpy(pGenHandle->name, comparator->handle);
        		pGenHandle->generalized_column.column_type = RESULT;
        		pGenHandle->generalized_column.column_pointer.result = resultIndices;
        		context->chandles_in_use++;
        		if(column->data != NULL)
        			free(column->data);
        		free(pGenColumn);
        		free(comparator);
        	}
            break;
        case FETCH:

        	column = query->operator_fields.fetch_operator.column;
        	char* handle = query->operator_fields.fetch_operator.handle;
        	int numCHandles = context->chandles_in_use;
        	GeneralizedColumnHandle* pGenHandle = NULL;
        	for(j=0; j<numCHandles; j++){
        		pGenHandle = &(context->chandle_table[j]);
        		if(strcmp(pGenHandle->name, handle) == 0 && 
        			(pGenHandle->generalized_column.column_type == RESULT)){
        			Result* pResult = pGenHandle->generalized_column.column_pointer.result;
        			int *resultColValues = (int*)malloc(sizeof(int) * (pResult->num_tuples));
        			int* pPayload = (int*)pResult->payload;
        			int k = 0;
        			for(i=0; i<(pResult->num_tuples); i++){
        				resultColValues[k++] = column->data[pPayload[i]];
        			}
        			if(context->chandles_in_use == context->chandle_slots){
			            context->chandle_slots *= 2; 
			            context->chandle_table = (GeneralizedColumnHandle*)
			                            realloc(context->chandle_table, (context->chandle_slots) * sizeof(GeneralizedColumnHandle));
        			}
        			GeneralizedColumnHandle* pGenHandle = &(context->chandle_table[context->chandles_in_use]);
	        		strcpy(pGenHandle->name, handle);
	        		pGenHandle->generalized_column.column_type = RESULT;
	        		pGenHandle->generalized_column.column_pointer.result = (void*)resultColValues;
	        		context->chandles_in_use++;
	        		if(column->data != NULL)
        				free(column->data);
        		}
        	}
            break;
        case PRINT:
            break;
    }
    if(query->operator_fields.insert_operator.values != NULL)
        free(query->operator_fields.insert_operator.values);
    free(query);
    return;
}