#ifndef CLIENT_CONTEXT_H
#define CLIENT_CONTEXT_H

#include "cs165_api.h"

#define MAX_RESPONSE_SIZE 8192

Table* lookup_table(char *name);

Column* retrieve_column_for_scan(Table* table, char* col_name);

#endif
