
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
