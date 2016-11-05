#ifndef CLIENT_CONTEXT_H
#define CLIENT_CONTEXT_H

#include "cs165_api.h"
#include "btree.h"

typedef struct ThreadScanData{
	void* data;
	size_t startIdx;
	size_t endIdx;
	Comparator* pComparator;
	GeneralizedColumnHandle* pGenHandle;
	size_t dataSize;
}ThreadScanData;

Table* lookup_table(char *name);

Column* retrieve_column_for_scan(Table* table, char* col_name, bool loadData);

void createSortColumnUnClusteredIndex(Table* table, Column* column);

void createTreeColumnUnClusteredIndex(Table* table, Column* column);

#endif
