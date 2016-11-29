#ifndef HASH_TABLE_GUARD
#define HASH_TABLE_GUARD

#define TABLE_SIZE 97
#define NODE_PAIR 2

#define TUNING_PARAM 1.0
#define EPSILON 1.0e-3

typedef int keyType;
typedef int valType;

typedef struct hashNode{
	keyType* key;
	valType* value;
	unsigned char pairSize;
	struct hashNode *next;
}hashNode;


typedef struct hashtable {
// define the components of the hash table here (e.g. the array, bookkeeping for number of elements, etc)
	hashNode **buckets;
	int numPairs;
	size_t table_size;
	float table_threshold;
} hashtable;


void init(hashtable** ht, size_t table_size);
void put(hashtable** ht, keyType key, valType value);
int get(hashtable* ht, keyType key, valType *values, int num_values);
void erase(hashtable* ht, keyType key);
void destroy_hash_table(hashtable* ht);
int hash(keyType key, hashtable* ht);

#endif
