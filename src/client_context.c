
#include "client_context.h"
#include <string.h>
#include <unistd.h>
#include "utils.h"
#include "thpool.h"
#include "hash_table.h"


Table* lookup_table(char *name) {
	size_t i;
	for(i=0; i<(current_db->tables_size); i++){
		if(strcmp(current_db->tables[i].name, name) == 0){
			return &(current_db->tables[i]);
		}
	}
	    
	return NULL;
}

Column* retrieve_column_for_scan(Table* table, char* colname, bool loadData){
	size_t i;
	size_t numColumns = table->col_count;
	for(i=0; i<numColumns; i++){
		if(strcmp(table->columns[i].name, colname) == 0){
			Column* scanColumn = &(table->columns[i]);
			if(loadData && scanColumn->data == NULL){
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
			}
			return scanColumn;
		}
	}

	return NULL;
}

int BinarySearchLessThan(Table* table, Column* column, Comparator* comparator){
	size_t columnSize = table->table_length;
	ColumnIndex* pIndex = column->index;
	dataRecord* colTuplesArr = pIndex->tuples;

	int tupleIndex = -1; 
	if(comparator->type2 == LESS_THAN){
		long int m = comparator->p_high;
		
		int l=0, u=columnSize-1, mid, k;
		bool valueFound = false;
	    while(l<=u){
			mid=(l+u)/2;
			dataRecord tuple = colTuplesArr[mid];
			if(m==tuple.val){
				for(k=mid-1; (int)k>=0; k--){
					dataRecord ctuple = colTuplesArr[k];
					if(ctuple.val < m){
						tupleIndex = k;
						valueFound = true;
						break;
					}
				}
				break;
			}
			else if(m<tuple.val){
			 	u=mid-1;
			}
			else
			 	l=mid+1;
	    }
	    if(valueFound){
	    	return tupleIndex;
	    }
	    else{
	    	if(u != (int)columnSize-1) 
	    		k = u+1;
	    	else
	    		k = u;
    		for(;(int)k>=0;k--){
				dataRecord ctuple = colTuplesArr[k];
				if(ctuple.val < m){
					tupleIndex = k;
					break;
				}
			}
			return tupleIndex;
	    }
    }
    return tupleIndex;
}

int BinarySearchGreaterThanOrEqual(Table* table, Column* column, Comparator* comparator){
	size_t columnSize = table->table_length;
	ColumnIndex* pIndex = column->index;
	dataRecord* colTuplesArr = pIndex->tuples;

	int tupleIndex = -1; 
	if(comparator->type1 == GREATER_THAN_OR_EQUAL){
		long int m = comparator->p_low;
		
		int l=0, u=columnSize-1, mid, k;
		bool valueFound = false;
	    while(l<=u){
			mid=(l+u)/2;
			dataRecord tuple = colTuplesArr[mid];
			if(m==tuple.val){
				tupleIndex = mid;
				valueFound = true;
				break;
			}
			else if(m<tuple.val){
			 	u=mid-1;
			}
			else
			 	l=mid+1;
	    }
	    if(valueFound){
	    	return tupleIndex;
	    }
	    else{
	    	if(l != 0) 
	    		k = l-1;
	    	else
	    		k = l;
    		for(;k<(int)columnSize;k++){
				dataRecord ctuple = colTuplesArr[k];
				if(ctuple.val >= m){
					tupleIndex = k;
					break;
				}
			}
			return tupleIndex;
	    }
    }
    return tupleIndex;
}

static Result* computeResultIndices(Table* table, Column* column, Comparator* comparator){
	size_t i=0, j=0;
	int k = 0;
	size_t columnSize = table->table_length;
	int *resultIndices = (int*)malloc(sizeof(int) * columnSize);
	memset(resultIndices, 0, sizeof(int)*columnSize);
	Result* pResult = (Result*)malloc(sizeof(Result));
	memset(pResult, 0, sizeof(Result));
	pResult->lower_idx = -1;
	pResult->upper_idx = -1;
	ColumnIndex* pIndex = column->index;
	
	if(comparator->type1 == NO_COMPARISON && comparator->type2 == NO_COMPARISON){
		if(pIndex != NULL){
			pResult->lower_idx = 0;
			pResult->upper_idx = columnSize-1;
		}else{
			for (i=0; i<columnSize; i++){
				resultIndices[j++]=i;
			}
		}
	}else if(comparator->type1 == NO_COMPARISON && comparator->type2 == LESS_THAN){
		if(pIndex != NULL){
			if(pIndex->clustered){
				if(pIndex->indexType == BTREE){

				}else{
					int retDataIndex = BinarySearchLessThan(table, column, comparator);
					if(retDataIndex != -1){
						pResult->lower_idx = 0;
						pResult->upper_idx = retDataIndex;
					}
				}
			}else if(pIndex->unclustered){
				if(pIndex->indexType == BTREE){

				}else{

				}
			}else{
				for (i=0; i<columnSize; i++){
					if(column->data[i] < comparator->p_high)
						resultIndices[j++]=i;
				}
			}
		}else{
			for (i=0; i<columnSize; i++){
				if(column->data[i] < comparator->p_high)
					resultIndices[j++]=i;
			}
		}
	}else if(comparator->type1 == GREATER_THAN_OR_EQUAL && comparator->type2 == NO_COMPARISON){
		if(pIndex != NULL){
			if(pIndex->clustered){
				if(pIndex->indexType == BTREE){

				}else{
					int retDataIndex = BinarySearchGreaterThanOrEqual(table, column, comparator);
					if(retDataIndex != -1){
						pResult->lower_idx = retDataIndex;
						pResult->upper_idx = columnSize-1;
					}
				}
			}else if(pIndex->unclustered){
				if(pIndex->indexType == BTREE){

				}else{
					
				}
			}
		}else{
			for (i=0; i<columnSize; i++){
				if(column->data[i] >= comparator->p_low)
					resultIndices[j++]=i;
			}
		}
	}else if(comparator->type1 == GREATER_THAN_OR_EQUAL && comparator->type2 == LESS_THAN){
		if(pIndex != NULL){
			if(pIndex->clustered){
				if(pIndex->indexType == BTREE){
					treeRoot* root = pIndex->dataIndex;
					int retDataIndexLow = find_lower_index_clustered(root, comparator->p_low);
					int retDataIndexHigh = find_higher_index_clustered(root, comparator->p_high);
					if(retDataIndexLow != -1 && retDataIndexHigh != -1){
						pResult->lower_idx = retDataIndexLow;
						pResult->upper_idx = retDataIndexHigh;
						j = (retDataIndexHigh - retDataIndexLow) + 1;
					}
				}else{
					int retDataIndexLow = BinarySearchGreaterThanOrEqual(table, column, comparator);
					int retDataIndexHigh = BinarySearchLessThan(table, column, comparator);
					if(retDataIndexLow != -1 && retDataIndexHigh != -1){
						pResult->lower_idx = retDataIndexLow;
						pResult->upper_idx = retDataIndexHigh;
						j = (retDataIndexHigh - retDataIndexLow) + 1;
					}
				}
			}else if(pIndex->unclustered){
				if(pIndex->indexType == BTREE){
					treeRoot* root = pIndex->dataIndex;
					j = find_result_indices_scan_unclustered_select(root, comparator->p_low, comparator->p_high, resultIndices);
				}else{
					int retDataIndexLow = BinarySearchGreaterThanOrEqual(table, column, comparator);
					int retDataIndexHigh = BinarySearchLessThan(table, column, comparator);
					if(retDataIndexLow != -1 && retDataIndexHigh != -1){
						dataRecord* tuples = pIndex->tuples;
						for (k=retDataIndexLow; k<=retDataIndexHigh; k++){
							resultIndices[j++]=(tuples[k].pos);
						}
					}
				}
			}else{
				for (i=0; i<columnSize; i++){
					if(column->data[i] >= comparator->p_low && column->data[i] < comparator->p_high)
						resultIndices[j++]=i;
				}
			}
		}else{
			for (i=0; i<columnSize; i++){
				if(column->data[i] >= comparator->p_low && column->data[i] < comparator->p_high)
					resultIndices[j++]=i;
			}
		}
	}
	
	pResult->payload = resultIndices;
	pResult->num_tuples = j;
	pResult->data_type = INT;
	return pResult;
}

static Result* ComputeResultIndicesType2(GeneralizedColumn* genColPos, GeneralizedColumn* genColval, Comparator* comparator){
	size_t i=0, j=0;

	Result* resultPos = genColPos->column_pointer.result;
	int* pListPos = (int*)(resultPos->payload);
	size_t columnSize = resultPos->num_tuples;

	int* pListVal = NULL;
	if(genColval->column_type == COLUMN)
	{
		Column* columnVal = genColval->column_pointer.column;
		pListVal = (int*)(columnVal->data);
	}
	else{
		Result* resultVal = genColval->column_pointer.result;
		pListVal = (int*)(resultVal->payload);
	}

	int *resultIndices = (int*)malloc(sizeof(int) * columnSize);
	memset(resultIndices, -1, sizeof(int)*columnSize);
	Result* pResult = (Result*)malloc(sizeof(Result));
	memset(pResult, 0, sizeof(Result));
	
	if(comparator->type1 == NO_COMPARISON && comparator->type2 == NO_COMPARISON){
		for (i=0; i<columnSize; i++){
			resultIndices[j++]=pListPos[i];
		}
	}else if(comparator->type1 == NO_COMPARISON && comparator->type2 == LESS_THAN){
		for (i=0; i<columnSize; i++){
			if(pListVal[i] < comparator->p_high)
				resultIndices[j++]=pListPos[i];
		}
	}else if(comparator->type1 == GREATER_THAN_OR_EQUAL && comparator->type2 == NO_COMPARISON){
		for (i=0; i<columnSize; i++){
			if(pListVal[i] >= comparator->p_low)
				resultIndices[j++]=pListPos[i];
		}
	}else if(comparator->type1 == GREATER_THAN_OR_EQUAL && comparator->type2 == LESS_THAN){
		for (i=0; i<columnSize; i++){
			if(pListVal[i] >= comparator->p_low && pListVal[i] < comparator->p_high)
				resultIndices[j++]=pListPos[i];
		}
	}
	
	pResult->payload = resultIndices;
	pResult->num_tuples = j;
	pResult->data_type = INT;
	return pResult;
}

static int* get_col_col_add_sub(Column* column1, Column* column2, size_t nData, bool isAdd){
	if(column1 == NULL || column2 == NULL){
		return NULL;
	}
	size_t i=0;
	int* opResult = (int*)malloc(sizeof(int)*nData);
	if(isAdd){
		for(i=0; i<nData; i++){
			opResult[i] = column1->data[i] + column2->data[i];
   		} 
	}
	else{
		for(i=0; i<nData; i++){
			opResult[i] = column1->data[i] - column2->data[i];
   		} 
	}
	if(column1->data != NULL)
		free(column1->data);
	if(column2->data != NULL)
		free(column2->data);
	
	return opResult;        		
}

static int* get_col_res_add_sub(Column* column1, Result* column2, size_t nData, bool isAdd){
	if(column1 == NULL || column2 == NULL){
		return NULL;
	}
	size_t i=0;
	int* opResult = (int*)malloc(sizeof(int)*nData);

	int* pData2 = (int*)column2->payload;
	if(isAdd){
		for(i=0; i<nData; i++){
			opResult[i] = column1->data[i] + pData2[i];
   		} 
	}
	else{
		for(i=0; i<nData; i++){
			opResult[i] = column1->data[i] - pData2[i];
   		} 
	}

	if(column1->data != NULL)
		free(column1->data);
	return opResult;    
}

static int* get_res_col_add_sub(Result* column1, Column* column2, size_t nData, bool isAdd){
	if(column1 == NULL || column2 == NULL){
		return NULL;
	}
	size_t i=0;
	int* opResult = (int*)malloc(sizeof(int)*nData);

	int* pData1 = (int*)column1->payload;
	if(isAdd){
		for(i=0; i<nData; i++){
			opResult[i] = pData1[i] + column2->data[i];
   		} 
	}
	else{
		for(i=0; i<nData; i++){
			opResult[i] = pData1[i] - column2->data[i];
   		} 
	}
	if(column2->data != NULL)
		free(column2->data);
	return opResult;    
}

static int* get_res_res_add_sub(Result* column1, Result* column2, size_t nData, bool isAdd){
	if(column1 == NULL || column2 == NULL){
		return NULL;
	}
	size_t i=0;
	int* opResult = (int*)malloc(sizeof(int)*nData);

	int* pData1 = (int*)column1->payload;
	int* pData2 = (int*)column2->payload;
	if(isAdd){
		for(i=0; i<nData; i++){
			opResult[i] = pData1[i] + pData2[i];
   		} 
	}
	else{
		for(i=0; i<nData; i++){
			opResult[i] = pData1[i] - pData2[i];
   		} 
	}

	return opResult;    
}

static int get_max_min_col_value(GeneralizedColumn* pGenColumn, size_t dataSize, bool isMax){
	int outVal;
	size_t i;
	if(pGenColumn->column_type == COLUMN){
		Column* column = pGenColumn->column_pointer.column;
		outVal = column->data[0];
		if(isMax){
			for(i=1; i<dataSize; i++){
				if(column->data[i] > outVal)
					outVal = column->data[i];
			}
		}
		else{
			for(i=1; i<dataSize; i++){
				if(column->data[i] < outVal)
					outVal = column->data[i];
			}
		}
		free(column->data);
	}else{
		Result* pResult = pGenColumn->column_pointer.result;
		int* resValues = (int*)(pResult->payload);
		outVal = resValues[0];
		if(isMax){
			for(i=1; i<dataSize; i++){
				if(resValues[i] > outVal)
					outVal = resValues[i];
			}
		}
		else{
			for(i=1; i<dataSize; i++){
				if(resValues[i] < outVal)
					outVal = resValues[i];
			}
		}
	}
	return outVal;
}
GeneralizedColumnHandle* check_for_existing_handle(ClientContext* context, char* handle){
	GeneralizedColumnHandle* pGenHandle = NULL;
	int j=0;
	int numCHandles = context->chandles_in_use;
	for(j=0; j<numCHandles; j++){
		pGenHandle = &(context->chandle_table[j]);
		if(strcmp(pGenHandle->name, handle) == 0 && 
			(pGenHandle->generalized_column.column_type == RESULT)){
			return pGenHandle;
		}
	}
	return NULL;
}

//Filling the index data for the clustered column
static void fillDataIndex(Column* column, size_t columnSize){
	size_t i;
	ColumnIndex* pIndex = column->index;
	int* baseData = column->data;
	//copy base data into index data
	if(columnSize >= pIndex->index_data_capacity){
		pIndex->index_data_capacity = columnSize + 1;
		pIndex->tuples = (dataRecord*)realloc(pIndex->tuples, (pIndex->index_data_capacity) * sizeof(dataRecord));
	}

	for(i=0; i<columnSize; i++){
		dataRecord* colTuple = &(pIndex->tuples[i]);
		colTuple->pos = i;
		colTuple->val = baseData[i];
	}
}

static int cmppos(const void *p1, const void *p2){
	const int *ia = (int *)p1;
	const int *ib = (int *)p2;

	return (*ia - *ib);
}

static int cmprecordval(const void *p1, const void *p2){
	const dataRecord *ia = (dataRecord *)p1;
	const dataRecord *ib = (dataRecord *)p2;

	return (ia->val - ib->val);
}

// static int cmprecordpos(const void *p1, const void *p2){
// 	const dataRecord *ia = (dataRecord *)p1;
// 	const dataRecord *ib = (dataRecord *)p2;

// 	return (ia->pos - ib->pos);
// }

void createSortClusteredColumnIndex(Table* table, Column* column){
	size_t columnSize = table->table_length;
	ColumnIndex* pIndex = column->index;

	fillDataIndex(column, columnSize);

	dataRecord* colTuplesArr = pIndex->tuples;
	qsort(colTuplesArr, columnSize, sizeof(dataRecord), cmprecordval);
}

void createTreeClusteredColumnIndex(Table* table, Column* column){
	size_t i;
	size_t columnSize = table->table_length;
	ColumnIndex* pIndex = column->index;
	int* baseData = column->data;
	treeRoot* root = NULL;
	if(pIndex->dataIndex == NULL)
	{
		root = (treeRoot*)malloc(sizeof(treeRoot));
		memset(root, 0, sizeof(treeRoot));
	}
	else 
		root = pIndex->dataIndex;
	for(i=0; i<columnSize; i++){
		root = insert_in_treeN(root, baseData[i], i);
	}
	pIndex->dataIndex = root;
	if(columnSize >= pIndex->index_data_capacity){
		pIndex->index_data_capacity = columnSize + 1;
		pIndex->tuples = (dataRecord*)realloc(pIndex->tuples, (pIndex->index_data_capacity) * sizeof(dataRecord));
	}
	getTreeDataRecords(root, &(pIndex->tuples));
}

void createSortColumnUnClusteredIndex(Table* table, Column* column){
	size_t columnSize = table->table_length;
	ColumnIndex* pIndex = column->index;
	dataRecord** colTuplesArr = &(pIndex->tuples);

	//fillDataIndex(column, columnSize);
	qsort(*colTuplesArr, columnSize, sizeof(dataRecord), cmprecordval);
}

void createTreeColumnUnClusteredIndex(Table* table, Column* column){
	size_t i;
	size_t columnSize = table->table_length;

	ColumnIndex* pIndex = column->index;
	dataRecord* colTuplesArr = pIndex->tuples;
	treeRoot* root = NULL;
	if(pIndex->dataIndex == NULL)
	{
		root = (treeRoot*)malloc(sizeof(treeRoot));
		memset(root, 0, sizeof(treeRoot));
	}
	else 
		root = pIndex->dataIndex;
	for(i=0; i<columnSize; i++){
		dataRecord* lColTuple = &(colTuplesArr[i]);
		int basePos = lColTuple->pos;
		int value = lColTuple->val;
		root = insert_in_treeN(root, value, basePos);
	}
	pIndex->dataIndex = root;
	if(columnSize >= pIndex->index_data_capacity){
		pIndex->index_data_capacity = columnSize + 1;
		dataRecord* tempTuples = (dataRecord*)realloc(pIndex->tuples, (pIndex->index_data_capacity) * sizeof(dataRecord));
		pIndex->tuples = tempTuples;
	}
}

void create_unclustered_index(Table* table, bool isAnyClusteredIndex){
	size_t idx = 0;
	size_t numColumns = table->col_count;
	size_t columnSize = table->table_length;
	//Create unclustered index
	while(idx < numColumns){
		Column* chcolumn = &(table->columns[idx]);
		ColumnIndex* pchIndex = chcolumn->index;
	    if(pchIndex != NULL && pchIndex->unclustered){
	    	//Fill the data tuples for table with only unclustered index
	    	if(!isAnyClusteredIndex){
		    	if(columnSize > pchIndex->index_data_capacity){
					pchIndex->index_data_capacity = columnSize + 1;
	        		dataRecord* tempTuples =(dataRecord*) realloc(pchIndex->tuples,
	        								(pchIndex->index_data_capacity)*sizeof(dataRecord));
	        		pchIndex->tuples = tempTuples;
	        	}
				int* baseData = chcolumn->data;
				size_t k=0;
				for(k=0; k<columnSize; k++){
					dataRecord* tColTuple = &(pchIndex->tuples[k]);
					tColTuple->pos = k;
					tColTuple->val = baseData[k];
				}
			}

	    	if(pchIndex->indexType == SORTED){
				createSortColumnUnClusteredIndex(table, chcolumn);
			}
			else if(pchIndex->indexType == BTREE){
				createTreeColumnUnClusteredIndex(table, chcolumn);
			}
	    }
	    idx++;
	}
}

void update_column_index(Table* table, int* values){
	size_t i=0, j=0, k = 0, col_Idx = 0, idx = 0;;
	size_t numColumns = table->col_count;
	size_t columnSize = table->table_length;
	size_t colNumFirstClust = -1;

	for(col_Idx = 0; col_Idx<numColumns; col_Idx++){
		Column* column = &(table->columns[col_Idx]);
		ColumnIndex* pIndex = column->index;
	    if(pIndex != NULL && pIndex->clustered){
	    	if(strcmp(table->firstDeclaredClustCol, column->name) == 0){
				colNumFirstClust = col_Idx;
			}

			if(columnSize >= pIndex->index_data_capacity){
				for(i=0; i<numColumns; i++){
					Column* pColumn = &(table->columns[i]);
					ColumnIndex* pColumnIndex = pColumn->index;
					pColumnIndex->index_data_capacity = columnSize + 1;
					dataRecord* tempTuples = (dataRecord*)realloc(pColumnIndex->tuples, 
											 		(pColumnIndex->index_data_capacity)*sizeof(dataRecord));
					pColumnIndex->tuples = tempTuples;
				}
			}

	    	if(pIndex->indexType == SORTED){
	    		int* baseSortData = column->data;
				//copy base data into index data

				//Fill the data in the end of the index tuples
				dataRecord* colTuple = &(pIndex->tuples[columnSize-1]);
				if(colTuple == NULL)
					return;
				colTuple->pos = columnSize-1;
				colTuple->val = baseSortData[columnSize-1];

				//Now sort the index data
				dataRecord** colTuplesArr = &(pIndex->tuples);
				qsort(*colTuplesArr, columnSize, sizeof(dataRecord), cmprecordval);
	    	}else if(pIndex->indexType == BTREE){
				
			}

			//Propagate the order
		 	char* leadingColName = table->firstDeclaredClustCol;
		    if(strlen(leadingColName) > 0){
		    	Column* leadingCol = &(table->columns[colNumFirstClust]);
		    	ColumnIndex* leadingColIndex = leadingCol->index;
		    	for(k=0; k<columnSize; k++){
					dataRecord* lColTuple = &(leadingColIndex->tuples[k]);
					size_t basePos = lColTuple->pos;
					for(j=0; j<numColumns; j++){
						if(j == colNumFirstClust)
							continue;
						Column* tCol = &(table->columns[j]);
						ColumnIndex* tIndex = tCol->index;
						if(tIndex->unclustered)
							continue;
						int* baseData = tCol->data;
						dataRecord* tColTuple = &(tIndex->tuples[k]);
						tColTuple->pos = basePos;
						tColTuple->val = baseData[basePos];
					}
				}
		    }
	    }
	}

	//Now update unclustered index
	while(idx < numColumns){
		Column* chcolumn = &(table->columns[idx]);
		ColumnIndex* pchIndex = chcolumn->index;
	    if(pchIndex != NULL && pchIndex->unclustered){
	    	if(pchIndex->indexType == SORTED){
			}
			else if(pchIndex->indexType == BTREE){
				
				treeRoot* root = NULL;
				if(pchIndex->dataIndex == NULL)
				{
					root = (treeRoot*)malloc(sizeof(treeRoot));
					memset(root, 0, sizeof(treeRoot));
				}
				else 
					root = pchIndex->dataIndex;
				int basePos = columnSize-1;
				int value = values[idx];
				root = insert_in_treeN(root, value, basePos);
				getTreeDataRecords(root, &(pchIndex->tuples));
			}
	    }
	    idx++;
	}

}

void form_column_index(Table* table){
	size_t i=0, j=0, k = 0, col_Idx = 0;
	size_t numColumns = table->col_count;
	size_t columnSize = table->table_length;
	size_t colNumFirstClust = -1;
	bool isAnyClusteredIndex = false;

	for(col_Idx = 0; col_Idx<numColumns; col_Idx++){
		Column* column = &(table->columns[col_Idx]);
		ColumnIndex* pIndex = column->index;
	    if(pIndex != NULL && pIndex->clustered){
	    	isAnyClusteredIndex = true;
			if(strcmp(table->firstDeclaredClustCol, column->name) == 0){
				colNumFirstClust = col_Idx;
			}
			if(pIndex->indexType == SORTED){
				createSortClusteredColumnIndex(table, column);
			}
			else if(pIndex->indexType == BTREE){
				createTreeClusteredColumnIndex(table, column);
			}

		    for(i=0; i<numColumns; i++){
		        Column* otherColumn = &(table->columns[i]);
		        ColumnIndex* pOtherColumnIndex = otherColumn->index;
		        if(pOtherColumnIndex == NULL){
		        	otherColumn->index = (ColumnIndex*)malloc(sizeof(ColumnIndex));
		            memset(otherColumn->index, 0, sizeof(ColumnIndex));
		            pOtherColumnIndex = otherColumn->index;
		            pOtherColumnIndex->index_data_capacity = columnSize;
		            pOtherColumnIndex->tuples = (dataRecord*)
		            							malloc(sizeof(dataRecord)*(pOtherColumnIndex->index_data_capacity));
		            memset(pOtherColumnIndex->tuples, 0, sizeof(dataRecord)*(pOtherColumnIndex->index_data_capacity));
		        }else{
		        	if(!(pOtherColumnIndex->clustered)){
		        		if(columnSize > pOtherColumnIndex->index_data_capacity){
							pOtherColumnIndex->index_data_capacity = columnSize + 1;
		            		pOtherColumnIndex->tuples=(dataRecord*)
		            			realloc(pOtherColumnIndex->tuples,
		            				(pOtherColumnIndex->index_data_capacity)*sizeof(dataRecord));
		            	}
		        	}
		        }
		    }

		    //Propagate the order
		 	char* leadingColName = table->firstDeclaredClustCol;
		    if(strlen(leadingColName) > 0){
		    	Column* leadingCol = &(table->columns[colNumFirstClust]);
		    	ColumnIndex* leadingColIndex = leadingCol->index;
		    	for(k=0; k<columnSize; k++){
					dataRecord* lColTuple = &(leadingColIndex->tuples[k]);
					size_t basePos = lColTuple->pos;
					for(j=0; j<numColumns; j++){
						if(j == colNumFirstClust)
							continue;
						Column* tCol = &(table->columns[j]);
						int* baseData = tCol->data;
						ColumnIndex* tIndex = tCol->index;
						dataRecord* tColTuple = &(tIndex->tuples[k]);
						tColTuple->pos = basePos;
						tColTuple->val = baseData[basePos];
					}
				}
		    }
		}
	}

	create_unclustered_index(table, isAnyClusteredIndex);
	
}

void shared_scan(void* threadScanData){
	ThreadScanData* scanData = (ThreadScanData*)threadScanData;
	int* data = scanData->data;
    Comparator* selComp = scanData->pComparator;
    GeneralizedColumnHandle* pGenHandle = scanData->pGenHandle;

    size_t i=0, j=0;

	//For storing result of current processing
	int *resultIndices = NULL;
	Result* pResult = NULL;

	//Check if some data has already been processed in the previous run.
	if(pGenHandle->generalized_column.column_pointer.result == NULL)
    {
		pResult = (Result*)malloc(sizeof(Result));
		memset(pResult, 0, sizeof(Result));
		resultIndices = (int*)malloc(sizeof(int) * 1000);
		memset(resultIndices, 0, sizeof(int)*1000);
		pResult->data_type = INT;
		pResult->payload = resultIndices;
		pGenHandle->generalized_column.column_pointer.result = pResult;
	}
	else{
		pResult = pGenHandle->generalized_column.column_pointer.result;
		j = pResult->num_tuples;
		resultIndices = pResult->payload;
	}
	
	for (i=scanData->startIdx; i<scanData->endIdx; i++){
		if(data[i] >= selComp->p_low && data[i] < selComp->p_high)
			resultIndices[j++]=i;
	}
	
	pResult->lower_idx = -1;
	pResult->upper_idx = -1;
	pResult->num_tuples = j;
}

void computeHashJoinPositions(Result* pResultOuterVal, Result* pResultOuterPos,
							  Result* pResultInnerVal, Result* pResultInnerPos, 
							  int* posOuter, int* posInner, size_t* num_positions){
	size_t num_tuples_Outer = pResultOuterVal->num_tuples;
	size_t num_tuples_Inner = pResultInnerVal->num_tuples;

	int* pPayloadOuterVal = (int*)(pResultOuterVal->payload);
	int* pPayloadInnerVal = (int*)(pResultInnerVal->payload);

	int* pPayloadOuterPos = (int*)(pResultOuterPos->payload);
	int* pPayloadInnerPos = (int*)(pResultInnerPos->payload);

	size_t i=0, j=0, r=0, k=0;
	size_t cache_size = 24*1024/sizeof(int);  //32K-(2*4K) (L1-2 size)
	hashtable* ht = NULL;
	init(&ht, TABLE_SIZE);
	for (i=0;i<num_tuples_Inner;i++, k++){
		put(&ht, pPayloadInnerVal[i], pPayloadInnerPos[i]);
		if(k == cache_size/2 || (i == (num_tuples_Inner -1))){
			k = 0;
			for(j=0; j<num_tuples_Outer; j++){
				int num_values = 1;
				valType* values = malloc(1 * sizeof(valType));
				int num_results = get(ht, pPayloadOuterVal[j], values, num_values);
				if (num_results > 0) {
					if(num_results > num_values){
						values = realloc(values, num_results * sizeof(valType));
	    				get(ht, 0, values, num_results);
	    				int m=0;
	    				for(m=0; m<num_results; m++){
	    					posOuter[r] = pPayloadOuterPos[j];
							posInner[r++] = values[m];
	    				}
    				}else{
						posOuter[r] = pPayloadOuterPos[j];
						posInner[r++] = values[0];
					}
				}
				free(values);
			}
			destroy_hash_table(ht); ht = NULL;
			init(&ht, TABLE_SIZE);
		}
	}
	if(ht != NULL){
		destroy_hash_table(ht); ht = NULL;
	}
	if(r>0)
		*num_positions = r;
}

void computeNestedLoopJoinPositions(Result* pResultOuter, Result* pResultInner, int* posOuter, int* posInner, size_t* num_positions){
	size_t page_size = sysconf(_SC_PAGESIZE);
	size_t num_tuples_Outer = pResultOuter->num_tuples;
	size_t num_tuples_Inner = pResultInner->num_tuples;

	int* pPayloadOuter = (int*)(pResultOuter->payload);
	int* pPayloadInner = (int*)(pResultInner->payload);
	size_t i=0, j=0, r=0, m=0, k=0, lim1 = 0, lim2 = 0;
	for (i=0;i<num_tuples_Outer;i=i+page_size){
		for (j=0;j<num_tuples_Inner;j=j+page_size){
			if( ((int)(i+page_size) - (int)num_tuples_Outer) >= 0 ) lim1 = num_tuples_Outer;
			else lim1 = i+page_size;

			if( ((int)(j+page_size) - (int)num_tuples_Inner) >= 0 ) lim2 = num_tuples_Inner;
			else lim2 = j+page_size;

			for (r=i;r<lim1;r++){
				for (m=j;m<lim2;m++){
					if (pPayloadOuter[r]==pPayloadInner[m]){
						posOuter[k]=r;
						posInner[k++]=m;
					}
				}
			}
		}
	}
	if(k>0)
		*num_positions = k;
}

// void update_val_index(DbOperator* query){
// 	table = query->operator_fields.update_operator.table;
//     //size_t numColumns = table->col_count;
//     column = query->operator_fields.update_operator.column;
//     GeneralizedColumn* pGenResColumn = query->operator_fields.update_operator.gen_res_col;
//     int update_val = query->operator_fields.update_operator.update_val;

//     Result* pResult = pGenResColumn->column_pointer.result;
//     size_t num_tuples = pResult->num_tuples;
//     int* selectPos = (int*)(pResult->payload);


// }
/** execute_DbOperator takes as input the DbOperator and executes the query.
 **/
void execute_DbOperator(DbOperator* query, char** msg) {
    
    char defaultMsg[] = "--\n";
    strcpy(*msg, defaultMsg);
    if(!query)
        return;

    size_t i;
    int j, k;
    char** handleNames;
    ClientContext* context = query->context;
    Comparator* comparator = NULL;
    Table* table = NULL;
    Column* column = NULL;
    switch(query->type){
    	case CREATE_IDX:
    	{
    		table = query->operator_fields.create_idx_operator.table;
    		
    		form_column_index(table);

    		break;
    	}
        case INSERT: 
        {
            table = query->operator_fields.insert_operator.table;
            size_t numColumns = table->col_count;

            size_t columnSize = table->table_length;
            //check if the column data size need to be modified.
            if(columnSize == table->col_data_capacity){
            	table->col_data_capacity *= 2; 
            	for(i=0; i<numColumns; i++){
            		column = &(table->columns[i]);
            		if(column->data == NULL){
            			column = retrieve_column_for_scan(table, column->name, true);
            		}
            		column->data = (int*)realloc(column->data, (table->col_data_capacity) * sizeof(int));
            	}
            }
            
            //Copy the values into column data.
            for(i=0; i<numColumns; i++){
                column = &(table->columns[i]);
                column->data[columnSize] = query->operator_fields.insert_operator.values[i];
            }
            (table->table_length)++;

            update_column_index(table, query->operator_fields.insert_operator.values);
            //form_column_index(table);

            char insertMsg[] = "Insert Operation Succeeded";
            char str[128];
            memset(str, '\0', 128);
            sprintf(str, "-- %s Column Size: %zu, First Col value: %d\n",
                     insertMsg, table->table_length, query->operator_fields.insert_operator.values[0]);
            strcpy(*msg, str);

            if(query->operator_fields.insert_operator.values != NULL)
        		free(query->operator_fields.insert_operator.values);
           
            break;
        }
        case UPDATE:
        {
   //      	table = query->operator_fields.update_operator.table;
   //          //size_t numColumns = table->col_count;
   //          column = query->operator_fields.update_operator.column;
   //          GeneralizedColumn* pGenResColumn = query->operator_fields.update_operator.gen_res_col;
   //          int update_val = query->operator_fields.update_operator.update_val;

   //          Result* pResult = pGenResColumn->column_pointer.result;   
   //          //int* colData = column->data;
   //       	ColumnIndex* pIndex = column->index;
			// if(pIndex != NULL){
			// 	if((pResult->upper_idx - pResult->lower_idx + 1) == (int)pResult->num_tuples){
   //  				if(pResult->lower_idx != -1 && pResult->upper_idx != -1){
   //  		// 			size_t dataIndexLow = pResult->lower_idx;
   //  		// 			dataRecord* lColTuple = &(pIndex->tuples[dataIndexLow]);
   //    //   				lColTuple->val = update_val;
			// 			// int basePos = lColTuple->pos;
			// 			// colData[basePos] = update_val;
			// 		}
			// 	}else{
			// 		//int* resPayload = (int*)pResult->payload;
			// 		//dataRecord* lColTuple = &(pIndex->tuples[dataIndexLow]);
			// 	}
   //  		}
        	break;
        }
        case DELETE:
        {
        	table = query->operator_fields.delete_operator.table;
        	GeneralizedColumn* pGenResColumn = query->operator_fields.delete_operator.gen_res_col;

        	Result* pResult = pGenResColumn->column_pointer.result;   
  			int numColumns = table->col_count;
            size_t columnSize = table->table_length;
            for(j=0; j<numColumns; j++){
            	column = &(table->columns[j]);
            	if(column->data == NULL){
        			column = retrieve_column_for_scan(table, column->name, true);
        		}
            }
            if(((pResult->upper_idx - pResult->lower_idx + 1) == (int)pResult->num_tuples) && (pResult->num_tuples > 1))
            {
				if(pResult->lower_idx != -1 && pResult->upper_idx != -1){
					for(j=0; j<numColumns; j++){
						column = &(table->columns[j]);
						ColumnIndex* pIndex = column->index;
						dataRecord* colTuplesArr = pIndex->tuples;
						for(i=(size_t)(pResult->lower_idx); i<=(size_t)(pResult->upper_idx); i++){
							int value = (colTuplesArr[i]).val;
							int pos = (colTuplesArr[i]).pos;
         
			                column->data[pos] = (1<<30);             			
	
		                	if(pIndex->indexType == BTREE){
		                		delete_key(pIndex->dataIndex, value);
		                	}else{
		                		dataRecord* pTupleRecord = &(pIndex->tuples[i]);
		                		pTupleRecord->val = (1<<30);
		                	}
		                }	
					}
				}
			}else{
				if(pResult->num_tuples > 0){
					int *pPayload = pResult->payload;
					qsort(pPayload, pResult->num_tuples, sizeof(int), cmppos);
					for(j=0; j<numColumns; j++){
						column = &(table->columns[j]);
						for(i=0; i<(pResult->num_tuples); i++){
	    					column->data[pPayload[i]] = (1<<30); 
	    				}
	    			}
	    			for(j=0; j<numColumns; j++){
	    				column = &(table->columns[j]);
						ColumnIndex* pIndex = column->index;
						if(pIndex != NULL){
							dataRecord* colTuplesArr = pIndex->tuples;
							for(i=0; i<(pResult->num_tuples); i++){
								int pos = pPayload[i];
								for(k=0; k<(int)columnSize; k++){
									if(colTuplesArr[k].pos == pos){
										if(pIndex->indexType == BTREE){
		                					delete_key(pIndex->dataIndex, colTuplesArr[k].val);
										}
	                					else{
											dataRecord* pTupleRecord = &(pIndex->tuples[k]);
			                				pTupleRecord->val = (1<<30);
		                				}
									}
								}
							}
						}
	    			}
				}
			}

        	break;
        }
        case CREATE:
            break;
        case OPEN:
            break;
        case BATCH:
        {
        	BatchOperator* pOperator = context->batchOperator;
        	int numOperators = pOperator->numSelOperators;
        	SelectOperator* pSelOperators = pOperator->selOperators;
        	if( numOperators <= 0)
        		break;

        	//Get a comparator from any select operator.
        	//Assumption that batch is being run on the same column.
        	comparator = pSelOperators[0].comparator;
        	GeneralizedColumn* pGenColumn = comparator->gen_col;
        	column = pGenColumn->column_pointer.column;

        	table = pSelOperators[0].table;
        	size_t columnSize = table->table_length;
        	int* data = column->data;

        	//divide the data into page sizes
        	//Each operator gets 5% of the page size for result
        	size_t system_page_size = sysconf(_SC_PAGESIZE);
        	size_t page_size = system_page_size - (numOperators*system_page_size*0.05);
        	size_t num_pages = (columnSize<<2)/page_size;
        	//data_rem is #column elements for the last page
        	size_t data_rem = ((columnSize<<2) % page_size)>>2;
        	//data_page is #column elements for full page
        	size_t data_page = page_size>>2;
        	num_pages++; //at least 1 page is required

        	//create the thread pool with #threads same as #operators.
        	threadpool* thpool = threadpool_init(numOperators);

        	//ThreadScanData is argument for a thread. Each array element would correspond to a thread.
        	ThreadScanData* pQueryScanDataArr = (ThreadScanData*) malloc(sizeof(ThreadScanData) * numOperators);
        	memset(pQueryScanDataArr, 0, sizeof(ThreadScanData) * numOperators);
        	for(j=0; j<numOperators; j++){
        		SelectOperator* pSelOperator = &(pSelOperators[j]);
        		Comparator* pComp = pSelOperator->comparator;
        		ThreadScanData* pScanData = &(pQueryScanDataArr[j]);
        		GeneralizedColumnHandle* pGenHandle = NULL;
	    		pGenHandle = check_for_existing_handle(context, pComp->handle);
	    		if(pGenHandle != NULL){
	    			if(pGenHandle->generalized_column.column_pointer.result != NULL)
		            {
		                if((pGenHandle->generalized_column.column_pointer.result)->payload != NULL)
		                    free((pGenHandle->generalized_column.column_pointer.result)->payload);
		                free(pGenHandle->generalized_column.column_pointer.result);
		            }
	    		}
	    		else{
	    			pGenHandle = &(context->chandle_table[context->chandles_in_use]);
	    			strcpy(pGenHandle->name, pComp->handle);
	    			context->chandles_in_use++;
	        		pGenHandle->generalized_column.column_type = RESULT;
	    		}

	    		pScanData->pGenHandle = pGenHandle;
        		pScanData->pComparator = pComp;
        		pScanData->data = data;
        	}

        	size_t dataSize;
        	size_t pageIdx = 0;
        	for(pageIdx=0; pageIdx<num_pages; pageIdx++){
        		dataSize = 0;
        		//Last page to process
        		if(pageIdx==num_pages-1)
        			dataSize = data_rem;
        		else
        			dataSize = data_page;
        		for(j=0; j<numOperators; j++){
        			ThreadScanData* pScanData = &(pQueryScanDataArr[j]);
        			pScanData->dataSize = dataSize;
        			pScanData->startIdx = pageIdx*data_page;
        			pScanData->endIdx = pageIdx*data_page + dataSize;
        			threadpool_add_work(thpool, shared_scan, (void*)pScanData);
        		}
        		threadpool_wait(thpool);
        	}
        	
        
        	break;
        }
        case JOIN:
        {
        	GeneralizedColumn* pGenColumn = query->operator_fields.join_operator.gen_col;
        	char outHandle[2][10];
        	strcpy(outHandle[0], query->operator_fields.join_operator.outhandle1);
        	strcpy(outHandle[1], query->operator_fields.join_operator.outhandle2);
        	char* join_method = query->operator_fields.join_operator.joinMethod;

        	//check which relation is outer (the bigger in tuples size)
        	Result* pResult0 = pGenColumn[0].column_pointer.result; //first fetch
        	Result* pResult2 = pGenColumn[2].column_pointer.result; //second fetch

        	Result* pResult1 = pGenColumn[1].column_pointer.result; //first select
        	Result* pResult3 = pGenColumn[3].column_pointer.result; //second select

        	size_t num_tuples0 = pResult0->num_tuples;
        	size_t num_tuples2 = pResult2->num_tuples;
        	size_t num_positions = 0;
        	int* pos0 = NULL, *pos2 = NULL;
        	if(strcmp(join_method, "nested-loop") == 0){
	        	if(num_tuples0 >= num_tuples2){
	        		pos0 = (int*)malloc(sizeof(int)*num_tuples2);
	        		pos2 = (int*)malloc(sizeof(int)*num_tuples2);
	        		computeNestedLoopJoinPositions(pResult0, pResult2, pos0, pos2, &num_positions);
	        	}else{
	        		pos0 = (int*)malloc(sizeof(int)*num_tuples0);
	        		pos2 = (int*)malloc(sizeof(int)*num_tuples0);
	        		computeNestedLoopJoinPositions(pResult2, pResult0, pos2, pos0, &num_positions);
	        	}
	        }else if(strcmp(join_method, "hash") == 0){
	        	if(num_tuples0 >= num_tuples2){
	        		pos0 = (int*)malloc(sizeof(int)*num_tuples2);
	        		pos2 = (int*)malloc(sizeof(int)*num_tuples2);
	        		computeHashJoinPositions(pResult0, pResult1, pResult2, pResult3, pos0, pos2, &num_positions);
	        	}else{
	        		pos0 = (int*)malloc(sizeof(int)*num_tuples0);
	        		pos2 = (int*)malloc(sizeof(int)*num_tuples0);
	        		computeHashJoinPositions(pResult2, pResult3, pResult0, pResult1, pos2, pos0, &num_positions);
	        	}
        	}
        	if(num_positions > 0){
        		int* firstIndices = (int*) malloc(sizeof(int)*num_positions);
        		int* secondIndices = (int*) malloc(sizeof(int)*num_positions);

        		if(strcmp(join_method, "hash") == 0){
        			memcpy(firstIndices, pos0, sizeof(int)*num_positions);
        			memcpy(secondIndices, pos2, sizeof(int)*num_positions);
        		}else{
	        		int* select1Pos = (int*)(pResult1->payload);
	        		int* select2Pos = (int*)(pResult3->payload);
	        		for(i=0; i<num_positions; i++){
	        			firstIndices[i] = select1Pos[pos0[i]];
	        			secondIndices[i] = select2Pos[pos2[i]];
	        		}
        		}

        		for(i=0; i<2; i++){
        			GeneralizedColumnHandle* pGenHandleNew = NULL;
	        		pGenHandleNew = check_for_existing_handle(context, outHandle[i]);
	        		if(pGenHandleNew != NULL){
	        			if(pGenHandleNew->generalized_column.column_pointer.result != NULL)
			            {
			                if((pGenHandleNew->generalized_column.column_pointer.result)->payload != NULL)
			                    free((pGenHandleNew->generalized_column.column_pointer.result)->payload);
			                free(pGenHandleNew->generalized_column.column_pointer.result);
			            }
	        		}
	        		else{
	        			pGenHandleNew = &(context->chandle_table[context->chandles_in_use]);
	        			context->chandles_in_use++;
	        			strcpy(pGenHandleNew->name, outHandle[i]);
	        			pGenHandleNew->generalized_column.column_type = RESULT;
	        		}

					Result* pResultNew = (Result*)malloc(sizeof(Result));
					memset(pResultNew, 0, sizeof(Result));
					if(i==0)
						pResultNew->payload = (void*)firstIndices;
					else
						pResultNew->payload = (void*)secondIndices;
					pResultNew->num_tuples = num_positions;
					pResultNew->data_type = INT;
	        		pGenHandleNew->generalized_column.column_pointer.result = pResultNew;
        		}
        	}
        	if(pos0 != NULL) free(pos0);
        	if(pos2 != NULL) free(pos2);
        	if(pGenColumn != NULL) free(pGenColumn);
        	
        	
        	break;
        }
        case SELECT:
        {
        	comparator = query->operator_fields.select_operator.comparator;
        	table = query->operator_fields.select_operator.table;
        	size_t nInputCols = comparator->numInputCols;
        	GeneralizedColumn* pGenColumn = comparator->gen_col;
        	Result* resultIndices = NULL;
        	if(nInputCols == 1){
	        	if( pGenColumn->column_type == COLUMN){
	        		column = pGenColumn->column_pointer.column;
	        		if(column == NULL){
	        			strcpy(*msg, "Column not found to scan");
	        			break;
	        		}
	        		resultIndices = computeResultIndices(table, column, comparator);
	        		if(column->data != NULL)
	        		{
	    				free(column->data);
	    				column->data = NULL;
	        		}
	        	}
	        }
	        else{
        		GeneralizedColumn* pGenColumnPos = &pGenColumn[0];
        		GeneralizedColumn* pGenColumnVal = &pGenColumn[1];
        		resultIndices = ComputeResultIndicesType2(pGenColumnPos, pGenColumnVal, comparator);
        	}
    		if(context->chandles_in_use == context->chandle_slots){
	            context->chandle_slots *= 2; 
	            context->chandle_table = (GeneralizedColumnHandle*)
	                            realloc(context->chandle_table, (context->chandle_slots) * sizeof(GeneralizedColumnHandle));
    		}
    		GeneralizedColumnHandle* pGenHandle = NULL;
    		pGenHandle = check_for_existing_handle(context, comparator->handle);
    		if(pGenHandle != NULL){
    			if(pGenHandle->generalized_column.column_pointer.result != NULL)
	            {
	                if((pGenHandle->generalized_column.column_pointer.result)->payload != NULL)
	                    free((pGenHandle->generalized_column.column_pointer.result)->payload);
	                free(pGenHandle->generalized_column.column_pointer.result);
	            }
    		}
    		else{
    			pGenHandle = &(context->chandle_table[context->chandles_in_use]);
    			strcpy(pGenHandle->name, comparator->handle);
    			context->chandles_in_use++;
        		pGenHandle->generalized_column.column_type = RESULT;
    		}
    		
    		pGenHandle->generalized_column.column_pointer.result = resultIndices;
 			free(comparator->handle);
    		free(pGenColumn);
    		free(comparator);
        	
            break;
        }

        case FETCH:
		{
        	column = query->operator_fields.fetch_operator.column;
        	char* handle = query->operator_fields.fetch_operator.handle;
        	char* targetHandle = query->operator_fields.fetch_operator.targetVecHandle;
        	int numCHandles = context->chandles_in_use;
        	GeneralizedColumnHandle* pGenHandle = NULL;
        	for(j=0; j<numCHandles; j++){
        		pGenHandle = &(context->chandle_table[j]);
        		if(strcmp(pGenHandle->name, targetHandle) == 0 && 
        			(pGenHandle->generalized_column.column_type == RESULT)){
        			Result* pResult = pGenHandle->generalized_column.column_pointer.result;
        			int *resultColValues = (int*)malloc(sizeof(int) * (pResult->num_tuples));
        			memset(resultColValues, 0, sizeof(int) * (pResult->num_tuples));
        			int* pPayload = (int*)pResult->payload;
        			ColumnIndex* pIndex = column->index;
        			int k = 0;
        			if(pIndex != NULL){
        				dataRecord* colTuplesArr = pIndex->tuples;
        				if((pResult->upper_idx - pResult->lower_idx + 1) == (int)pResult->num_tuples){
	        				if(pResult->lower_idx != -1 && pResult->upper_idx != -1){
		        				size_t dataIndexLow = pResult->lower_idx;
								size_t dataIndexHigh = pResult->upper_idx;
								for(i=dataIndexLow; i<=dataIndexHigh; i++){
									resultColValues[k++] = (colTuplesArr[i]).val;
								}
							}
						}else{
							qsort(pPayload, pResult->num_tuples, sizeof(int), cmppos);
							for(i=0; i<(pResult->num_tuples); i++){
	        					resultColValues[k++] = column->data[pPayload[i]];
	        				}
	        			}
        			}else{
	        			for(i=0; i<(pResult->num_tuples); i++){
	        				resultColValues[k++] = column->data[pPayload[i]];
	        			}
        			}
        			if(context->chandles_in_use == context->chandle_slots){
			            context->chandle_slots *= 5; 
			            context->chandle_table = (GeneralizedColumnHandle*)
			                            realloc(context->chandle_table, (context->chandle_slots) * sizeof(GeneralizedColumnHandle));
        			}
        			GeneralizedColumnHandle* pGenHandleNew = NULL;
	        		pGenHandleNew = check_for_existing_handle(context, handle);
	        		if(pGenHandleNew != NULL){
	        			if(pGenHandleNew->generalized_column.column_pointer.result != NULL)
			            {
			                if((pGenHandleNew->generalized_column.column_pointer.result)->payload != NULL)
			                    free((pGenHandleNew->generalized_column.column_pointer.result)->payload);
			                free(pGenHandleNew->generalized_column.column_pointer.result);
			            }
	        		}
	        		else{
	        			pGenHandleNew = &(context->chandle_table[context->chandles_in_use]);
	        			context->chandles_in_use++;
	        			strcpy(pGenHandleNew->name, handle);
	        			pGenHandleNew->generalized_column.column_type = RESULT;
	        		}

					Result* pResultNew = (Result*)malloc(sizeof(Result));
					memset(pResultNew, 0, sizeof(Result));
					pResultNew->payload = (void*)resultColValues;;
					pResultNew->num_tuples = k;
					pResultNew->data_type = INT;
	        		pGenHandleNew->generalized_column.column_pointer.result = pResultNew;
	        		if(column->data != NULL)
	        		{
        				free(column->data);
        				column->data = NULL;
	        		}
        			free(query->operator_fields.fetch_operator.handle);
        			free(query->operator_fields.fetch_operator.targetVecHandle);
        			break;
        		}
        	}
            break;
        }
        case PRINT:
    	{
        	handleNames = query->operator_fields.print_operator.handleNames;
        	int numHandles = query->operator_fields.print_operator.numHandles;
        	char* retStr = (char*)malloc(MAX_RESPONSE_SIZE);
        	memset(retStr, '\0', MAX_RESPONSE_SIZE);
        	//strcpy(retStr, " ");
        	int** valuesIntVec = (int**)malloc(sizeof(int*) * numHandles);
        	double** valuesDoubleVec = (double**)malloc(sizeof(double*) * numHandles);
        	long int** valuesLongVec = (long int**)malloc(sizeof(long int*) * numHandles);
        	int num_tuples = 0;
        	int iIndex = 0, fIndex = 0, lIndex = 0;;
        	DataType hTypes[numHandles];
        	Result* pResult = NULL;
        	for(j=0; j<numHandles; j++){
        		char* handleName = handleNames[j];
        		for(k=0; k<context->chandles_in_use; k++){
		            GeneralizedColumnHandle* pGenHandle = &(context->chandle_table[k]);
		            if(strcmp(pGenHandle->name, handleName) == 0 && pGenHandle->generalized_column.column_type == RESULT){
		            	pResult = pGenHandle->generalized_column.column_pointer.result;
		            	num_tuples = pResult->num_tuples;
		            	if(pResult->data_type == INT){
		            		valuesIntVec[iIndex++] = (int*)pResult->payload;
		            	}else if(pResult->data_type == DOUBLE){
		            		valuesDoubleVec[fIndex++] = (double*)pResult->payload;
		            	}else if(pResult->data_type == LONG){
		            		valuesLongVec[lIndex++] = (long int*)pResult->payload;
		            	}
		            	hTypes[j] = pResult->data_type;
		            	break;
					}
        		}
        	}
        	iIndex = 0; fIndex = 0;lIndex= 0;
        	for(j=0; j<num_tuples; j++){
        		for(k=0; k<numHandles; k++){
        			char str[20];
        			if(hTypes[k] == INT)
        				sprintf(str, "%d", valuesIntVec[iIndex++][j]);
        			else if(hTypes[k] == DOUBLE)
        				sprintf(str, "%.2f", valuesDoubleVec[fIndex++][j]);
        			else if(hTypes[k] == LONG)
        				sprintf(str, "%ld", valuesLongVec[lIndex++][j]);
        			if(k != numHandles-1)
        				strcat(str, ",");
        			strcat(retStr, str);
        		}
        		strcat(retStr, "\n");
        		iIndex = 0; fIndex = 0;lIndex = 0;
        	}
        	strcpy(*msg, retStr);
        	free(retStr);
        	for(k=0; k<numHandles; k++){
        		free(handleNames[k]);
        	}
        	free(valuesIntVec);
        	free(valuesDoubleVec);
        	free(valuesLongVec);
            break;
        }
        case MAX_MIN:
        {
        	char* outHandle = query->operator_fields.max_min_operator.handle;
        	bool isMax = query->operator_fields.max_min_operator.isMax;
        	GeneralizedColumn* pGenColumn = query->operator_fields.max_min_operator.gen_col;
        	size_t dataSize = query->operator_fields.max_min_operator.num_data;

        	if(dataSize == 0)
        		break;

        	int outValue = 0;
        	outValue = get_max_min_col_value(pGenColumn, dataSize, isMax);
        	Result* pResultNew = (Result*)malloc(sizeof(Result));
			memset(pResultNew, 0, sizeof(Result));
			pResultNew->num_tuples = 1;
			int* pPayloadInt = NULL;
			pPayloadInt = (int*)malloc(sizeof(int));
    		*pPayloadInt = (int)outValue;
    		pResultNew->payload = (void*)pPayloadInt;
			pResultNew->data_type = INT;

			if(context->chandles_in_use == context->chandle_slots){
	            context->chandle_slots *= 2; 
	            context->chandle_table = (GeneralizedColumnHandle*)
	                            realloc(context->chandle_table, (context->chandle_slots) * sizeof(GeneralizedColumnHandle));
			}

			GeneralizedColumnHandle* pGenHandleNew = NULL;
    		pGenHandleNew = check_for_existing_handle(context, outHandle);
    		if(pGenHandleNew != NULL){
    			if(pGenHandleNew->generalized_column.column_pointer.result != NULL)
	            {
	                if((pGenHandleNew->generalized_column.column_pointer.result)->payload != NULL)
	                    free((pGenHandleNew->generalized_column.column_pointer.result)->payload);
	                free(pGenHandleNew->generalized_column.column_pointer.result);
	            }
    		}
    		else{
    			pGenHandleNew = &(context->chandle_table[context->chandles_in_use]);
    			context->chandles_in_use++;
    			strcpy(pGenHandleNew->name, outHandle);
    			pGenHandleNew->generalized_column.column_type = RESULT;
    		}

    		pGenHandleNew->generalized_column.column_pointer.result = pResultNew;
    		free(pGenColumn);
			free(query->operator_fields.max_min_operator.handle);

			break;
        }
        case ADD_SUB:
        {
        	char* outHandle = query->operator_fields.add_sub_operator.handle;
        	bool isAdd = query->operator_fields.add_sub_operator.isAdd;
        	GeneralizedColumn* pGenColumn1 = query->operator_fields.add_sub_operator.gen_col1;
        	GeneralizedColumn* pGenColumn2 = query->operator_fields.add_sub_operator.gen_col2;
        	size_t dataSize = query->operator_fields.add_sub_operator.num_data;

        	int* outResult = NULL;
        	if(pGenColumn1->column_type == COLUMN){
        		if(pGenColumn2->column_type == COLUMN){
	        		outResult = get_col_col_add_sub(pGenColumn1->column_pointer.column, 
	        										pGenColumn2->column_pointer.column, dataSize, isAdd);
        		}else{
        			outResult = get_col_res_add_sub(pGenColumn1->column_pointer.column, 
	        										pGenColumn2->column_pointer.result, dataSize, isAdd);
        		}
        	}else if(pGenColumn1->column_type == RESULT){
        		if(pGenColumn2->column_type == COLUMN){
	        		outResult = get_res_col_add_sub(pGenColumn1->column_pointer.result,  
	        										pGenColumn2->column_pointer.column, dataSize, isAdd);
        		}else{
        			outResult = get_res_res_add_sub(pGenColumn1->column_pointer.result, 
	        										pGenColumn2->column_pointer.result, dataSize, isAdd);
        		}
        	}

        	Result* pResultNew = (Result*)malloc(sizeof(Result));
			memset(pResultNew, 0, sizeof(Result));
			pResultNew->num_tuples = dataSize;
			pResultNew->payload = (void*)outResult;

			if(context->chandles_in_use == context->chandle_slots){
	            context->chandle_slots *= 2; 
	            context->chandle_table = (GeneralizedColumnHandle*)
	                            realloc(context->chandle_table, (context->chandle_slots) * sizeof(GeneralizedColumnHandle));
			}

			GeneralizedColumnHandle* pGenHandleNew = NULL;
    		pGenHandleNew = check_for_existing_handle(context, outHandle);
    		if(pGenHandleNew != NULL){
    			if(pGenHandleNew->generalized_column.column_pointer.result != NULL)
	            {
	                if((pGenHandleNew->generalized_column.column_pointer.result)->payload != NULL)
	                    free((pGenHandleNew->generalized_column.column_pointer.result)->payload);
	                free(pGenHandleNew->generalized_column.column_pointer.result);
	            }
    		}
    		else{
    			pGenHandleNew = &(context->chandle_table[context->chandles_in_use]);
    			context->chandles_in_use++;
    			strcpy(pGenHandleNew->name, outHandle);
    			pGenHandleNew->generalized_column.column_type = RESULT;
    		}
    		pGenHandleNew->generalized_column.column_pointer.result = pResultNew;
    		free(pGenColumn1);
    		free(pGenColumn2);
			free(query->operator_fields.add_sub_operator.handle);

        	break;
        }
        case AVG_SUM:
        {
        	double avg_val = 0.0;
        	long int sum = 0.0;
        	char* handle = query->operator_fields.avg_sum_operator.handle;
        	bool isSum = query->operator_fields.avg_sum_operator.isSum;
        	GeneralizedColumn* pGenColumn = query->operator_fields.avg_sum_operator.gen_col;
        	size_t dataSize = query->operator_fields.avg_sum_operator.num_data;

        	if(pGenColumn == NULL)
        		return;

        	if(pGenColumn->column_type == COLUMN){
        		Column* column = pGenColumn->column_pointer.column;
        		for(i=0; i<dataSize; i++){
    				sum += column->data[i];
    			} 
    			avg_val = (double)sum/dataSize;
        	}else if(pGenColumn->column_type == RESULT){
        		Result* pResult = pGenColumn->column_pointer.result;
    			int* pPayload = (int*)pResult->payload;
    			for(i=0; i<dataSize; i++){
    				sum += pPayload[i];
    			}
    			avg_val = (double)sum/dataSize;
        	}else
        		return;

        	Result* pResultNew = (Result*)malloc(sizeof(Result));
			memset(pResultNew, 0, sizeof(Result));
			pResultNew->num_tuples = 1;

        	double* pPayloadAvg = NULL;
        	long int* pPayloadInt = NULL;
        	if(isSum){
        		pPayloadInt = (long int*)malloc(sizeof(int));
        		*pPayloadInt = (long int)sum;
        		pResultNew->payload = (void*)pPayloadInt;
				pResultNew->data_type = LONG;
        	}else{
        		pPayloadAvg = (double*)malloc(sizeof(double));
				*pPayloadAvg = avg_val;
				pResultNew->payload = (void*)pPayloadAvg;
				pResultNew->data_type = DOUBLE;
        	}
        	
        	if(context->chandles_in_use == context->chandle_slots){
	            context->chandle_slots *= 2; 
	            context->chandle_table = (GeneralizedColumnHandle*)
	                            realloc(context->chandle_table, (context->chandle_slots) * sizeof(GeneralizedColumnHandle));
			}

        	GeneralizedColumnHandle* pGenHandleNew = NULL;
    		pGenHandleNew = check_for_existing_handle(context, handle);
    		if(pGenHandleNew != NULL){
    			if(pGenHandleNew->generalized_column.column_pointer.result != NULL)
	            {
	                if((pGenHandleNew->generalized_column.column_pointer.result)->payload != NULL)
	                    free((pGenHandleNew->generalized_column.column_pointer.result)->payload);
	                free(pGenHandleNew->generalized_column.column_pointer.result);
	            }
    		}
    		else{
    			pGenHandleNew = &(context->chandle_table[context->chandles_in_use]);
    			context->chandles_in_use++;
    			strcpy(pGenHandleNew->name, handle);
    			pGenHandleNew->generalized_column.column_type = RESULT;
    		}

    		pGenHandleNew->generalized_column.column_pointer.result = pResultNew;
    		free(pGenColumn);
			free(query->operator_fields.avg_sum_operator.handle);
			break;
        }       
    }
    
    free(query);
    return;
}