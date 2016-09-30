
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

Column* retrieve_column(Table* table, char* col_name){
	size_t i;
	size_t numColumns = table->col_count;
	for(i=0; i<numColumns; i++){
		if(strcmp(table->columns[i].name, col_name) == 0){
			return &(table->columns[i]);
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
    ClientContext* context = query->context;
    Comparator* comparator = NULL;
    Table* table = NULL;
    Column* column = NULL;
    switch(query->type){
        case INSERT: 
            table = query->operator_fields.insert_operator.table;
            (table->table_length)++;
            size_t columnSize = table->table_length;
            
            size_t numColumns = table->col_count;
            for(i=0; i<numColumns; i++){
                column = &(table->columns[i]);
                if(columnSize == table->col_data_capacity){
                    table->col_data_capacity *= 2; 
                    column->data = (int*)realloc(column->data, (table->col_data_capacity) * sizeof(int));
                }
                column->data[columnSize-1] = query->operator_fields.insert_operator.values[i];
            }
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
        		GeneralizedColumnHandle* pGenHandle = &(context->chandle_table[(context->chandles_in_use)-1]);
        		pGenHandle->generalized_column.column_type = RESULT;
        		pGenHandle->generalized_column.column_pointer.result = resultIndices;
        		free(pGenColumn);
        		free(comparator);
        	}
            break;
        case FETCH:
            break;
        case PRINT:
            break;
    }
    if(query->operator_fields.insert_operator.values != NULL)
        free(query->operator_fields.insert_operator.values);
    free(query);
    return;
}