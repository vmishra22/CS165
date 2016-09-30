#ifndef CLIENT_CONTEXT_H
#define CLIENT_CONTEXT_H

#include "cs165_api.h"



Table* lookup_table(char *name);

Column* retrieve_column(Table* table, char* col_name);

#endif
