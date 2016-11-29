#include <stdlib.h>
#include <stdint.h>
#include <stdbool.h>
#include <string.h>
#include "hash_table.h"

int hash(keyType key, hashtable* ht){
	return key%(ht->table_size);
}

// initialize the components of the hashtable
void init(hashtable** ht, size_t table_size) {
    if(*ht == NULL){
    	*ht = calloc(1, sizeof(hashtable));
    	(*ht)->table_size = table_size;
    	(*ht)->buckets = (hashNode**)malloc(sizeof(hashNode*) * table_size);
    	size_t i=0;
    	for (i=0; i<table_size; i++){
    		(*ht)->buckets[i] = NULL;
    	}
    	(*ht)->numPairs = 0;
    	(*ht)->table_threshold = TUNING_PARAM*0.7;
	}
}

static void createNewNode(keyType key, valType value, hashNode** oNewNode){

	if(*oNewNode != NULL)
		return;
		
 	*oNewNode = (hashNode*) malloc(sizeof(hashNode));
    (*oNewNode)->key = (keyType*)calloc(NODE_PAIR, sizeof(keyType));
    (*oNewNode)->value = (valType*)calloc(NODE_PAIR, sizeof(valType));

	(*oNewNode)->key[0] = key;
	(*oNewNode)->value[0] = value;
	(*oNewNode)->pairSize = 1;
	(*oNewNode)->next = NULL;
}

static bool checkPrimeNumber(int num){
	int i = 0, flag = 0;
	for(i=2; i<=num/2; i++){
		if(num%i == 0){
			flag = 1;
			break;
		}
	}
	
	if(flag)
		return false;
		
	return true;
}

static int findNewTableSize(hashtable *ht){
	int newSize = 2*(ht->table_size);
	
	while(!checkPrimeNumber(newSize)){
		newSize++;
	}
	
	return newSize;
}

static hashtable* checkAndResizeHashTable(hashtable** ht){
	if(!ht)
		return NULL;
	
	hashtable *newht = NULL;
	hashtable* current_ht = *ht;
	float ratio = ((float)(current_ht->numPairs))/((float)(current_ht->table_size));
	float cur_threshold = TUNING_PARAM*ratio;
	
	if((cur_threshold - current_ht->table_threshold) > EPSILON){
		int newTableSize = findNewTableSize(current_ht);
		//new Hash table
		init(&newht, newTableSize);
		if(newht){
			//Iterate through old has table and rehash into the new table
			size_t i=0;
			for(i=0; i<(current_ht->table_size); i++){
				hashNode* head = current_ht->buckets[i];
				hashNode* cur_node = head;
				if(cur_node){
					do{
						int pairIndex = 0;
						for(pairIndex=0; pairIndex<(int)(cur_node->pairSize); pairIndex++){
							put(&newht, cur_node->key[pairIndex], cur_node->value[pairIndex]);
						}
					}while( (cur_node = cur_node->next) );
				}
			}
			destroy_hash_table(current_ht); current_ht = NULL;
			*ht = newht;
		}
	}
	return newht;
}

void destroy_hash_table(hashtable* ht){
	if(ht == NULL)
		return;
	size_t i=0;
	for(i=0; i<(ht->table_size); i++){
		hashNode* head = ht->buckets[i];
		hashNode* cur_node = head;
		if(cur_node){
			do{
				free(cur_node->key);
				free(cur_node->value);
				head = cur_node->next;
				free(cur_node);cur_node = NULL;
				cur_node = head;
			}while(head != NULL);
		}
	}
	free(ht->buckets);
	free(ht);
}

// insert a key-value pair into the hash table
void put(hashtable** cur_hashtable, keyType key, valType value) {
	if(cur_hashtable == NULL){
		return;
	}

	hashtable* ht = *cur_hashtable;
	int ht_index = 0;
	ht_index = hash(key, ht);
	
	if( !(ht->buckets[ht_index]) ){
	    hashNode *newNode = NULL;
	    createNewNode(key, value, &newNode);
		ht->buckets[ht_index] = newNode;
		ht->numPairs++;
		checkAndResizeHashTable(cur_hashtable);
	}
	else{
		hashNode* head = ht->buckets[ht_index];
			
		int headPairSize = head->pairSize;
		if(headPairSize < NODE_PAIR){
			ht->buckets[ht_index]->key[headPairSize] = key;
			ht->buckets[ht_index]->value[headPairSize] = value;
			headPairSize++;
			ht->buckets[ht_index]->pairSize = headPairSize;
			ht->numPairs++;
			checkAndResizeHashTable(cur_hashtable);
		}
		else{
			bool newNodeNeeded = true;
			hashNode* prev_node = head;
			hashNode* cur_node = head->next;
			int currentPairSize = 0;
			while(cur_node != NULL){
				currentPairSize = cur_node->pairSize;
				if(currentPairSize < NODE_PAIR){
					cur_node->key[currentPairSize] = key;
					cur_node->value[currentPairSize] = value;
					currentPairSize++;
					cur_node->pairSize = currentPairSize;
					ht->numPairs++;
					checkAndResizeHashTable(cur_hashtable);
					newNodeNeeded = false;
					break;
				}
				prev_node = cur_node;
				cur_node = cur_node->next;
			}
			if(newNodeNeeded)
			{
				hashNode *newNode = NULL;
				createNewNode(key, value, &newNode);
				prev_node->next = newNode;
				ht->numPairs++;
				checkAndResizeHashTable(cur_hashtable);
			}
		}
	}
}

// get entries with a matching key and stores the corresponding values in the
// values array. The size of the values array is given by the parameter
// num_values. If there are more matching entries than num_values, they are not
// stored in the values array to avoid a buffer overflow. The function returns
// the number of matching entries. If the return value is greater than
// num_values, the caller can invoke this function again (with a larger buffer)
// to get values that it missed during the first call. 
int get(hashtable* ht, keyType key, valType *values, int num_values) {
	int num_results = 0;
	
	int ht_index = hash(key, ht);
	hashNode* head = ht->buckets[ht_index];
	hashNode* cur_node = head;
	if(cur_node){
		do{
			int pairIndex = 0;
			
			for(pairIndex=0; pairIndex<(int)(cur_node->pairSize); pairIndex++){
				if(cur_node->key[pairIndex] == key){
					num_results++;
					if(num_results <= num_values ){
						values[num_results-1] = cur_node->value[pairIndex];
					}
				}
			}
			
		}while( (cur_node = cur_node->next) );
	}
	
    return num_results;
}

// erase a key-value pair from the hash talbe
void erase(hashtable* ht, keyType key) {
	int ht_index = hash(key, ht);
	
	hashNode* head = ht->buckets[ht_index];
	if(!head)
		return;
		
	hashNode* nextNode = NULL;
	int pairIndex = 0;
	//First check the key in the head of the list.
	for(pairIndex=0; pairIndex < (int)(head->pairSize); pairIndex++){
		if(head->key[pairIndex] == key){
			if(pairIndex != (head->pairSize)-1){
				memmove(ht->buckets[ht_index]->key+pairIndex, 
						ht->buckets[ht_index]->key+pairIndex+1,
					    ((head->pairSize) - (pairIndex+1))* sizeof(keyType));
				memmove(ht->buckets[ht_index]->value+pairIndex, 
						ht->buckets[ht_index]->value+pairIndex+1,
					    ((head->pairSize) - (pairIndex+1))* sizeof(valType));
			}
			head->pairSize--;
			ht->numPairs--;
		}
	}
	//If all entries of head removed then delete this head node.
	if(!(head->pairSize)){
		hashNode* temp = head;
		nextNode = temp->next;
		free(temp->key);
		free(temp->value);
		free(temp);
		ht->buckets[ht_index] = nextNode;
	}
	
	//Now check at the other nodes in the linked list.
	if(ht->buckets[ht_index] != NULL){
		hashNode* cur_node = ht->buckets[ht_index]->next;
		hashNode* prev_node = ht->buckets[ht_index];
		while(cur_node != NULL && prev_node != NULL){
			for(pairIndex=0; pairIndex < (int)(cur_node->pairSize); pairIndex++){
				if(cur_node->key[pairIndex] == key){
					if(pairIndex != (cur_node->pairSize)-1){
						memmove(cur_node->key+pairIndex, 
								cur_node->key+pairIndex+1,
								((cur_node->pairSize)-(pairIndex+1))* sizeof(keyType));
						memmove(cur_node->value+pairIndex, 
								cur_node->value+pairIndex+1,
								((cur_node->pairSize)-(pairIndex+1))* sizeof(valType));
					}
					cur_node->pairSize--;
					ht->numPairs--;
				}
			}
	
			if(!(cur_node->pairSize)){
				hashNode* temp = cur_node;
				prev_node->next = cur_node->next;
				free(temp->key);
				free(temp->value);
				free(temp);
			}
			prev_node = cur_node;
			cur_node = cur_node->next;
		}
	}
}









