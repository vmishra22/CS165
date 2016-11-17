#ifndef _BTREE_H_
#define _BTREE_H_

#include <stdlib.h>
#include <stdio.h>
#include <strings.h>
#include <stdbool.h>
#include "cs165_api.h"
#include <uuid/uuid.h>

#define MAX_ORDER 257

typedef struct leafNode {
	bool isLeaf;
	int num_keys;
	uuid_t node_id;
	struct leafNode* nextLeafNode;
	struct internalNode* parent;
	//void* queueNext;
	int* keys; //MAX_ORDER - 1    
	int* pos; //MAX_ORDER -1 
} leafNode;

typedef struct internalNode {
	bool isLeaf;
	int num_keys;
	uuid_t node_id;
	struct internalNode* parent;
	//void* queueNext;
	int* keys; //MAX_ORDER - 1 
	void** pointers;  //MAX_ORDER 
} internalNode;

//Tree root either points to an internal node or leaf node.
//May remove isLeaf and num_nodes attributes in the future.
typedef struct treeRoot{
	internalNode* iNode;
	leafNode* lNode;
	int num_nodes;
	bool isLeaf;
	int levels; //0 based
}treeRoot;

typedef struct record {
	int value;
	int pos;
} record;

typedef struct node {
	int node_id;
	void** pointers;
	int* keys;
	struct node * parent;
	bool is_leaf;
	int num_keys;
	struct node * next; 
} node;

// Output and utility.
// void enqueue( node * new_node );
// node * dequeue( void );
int height( node * root );
int path_to_root( node * root, node * child );
void print_leaves( node * root );
void print_tree( node * root );
void find_and_print(node * root, int key, bool verbose); 
void find_and_print_range(node * root, int range1, int range2, bool verbose); 
int find_range( node * root, int key_start, int key_end, bool verbose,
		int returned_keys[], void * returned_pointers[]); 
node * find_leaf( node * root, int key, bool verbose );
record * find( node * root, int key, bool verbose );
int cut( int length );

// Insertion.

record * make_record(int value, int pos) ;
node * make_node( void );
node * make_leaf( void );
int get_left_index(node * parent, node * left);
node * insert_into_leaf( node * leaf, int key, record * pointer );
node * insert_into_leaf_after_splitting(node * root, node * leaf, int key,
                                        record * pointer);
node * insert_into_node(node * root, node * parent, 
		int left_index, int key, node * right);
node * insert_into_node_after_splitting(node * root, node * parent,
                                        int left_index,
		int key, node * right);
node * insert_into_parent(node * root, node * left, int key, node * right);
node * insert_into_new_root(node * left, int key, node * right);
node * start_new_tree(int key, record * pointer);
node * insert_in_tree( node * root, int key, int value );

// Deletion.

int get_neighbor_index( node * n );
node * adjust_root(node * root);
node * coalesce_nodes(node * root, node * n, node * neighbor,
                      int neighbor_index, int k_prime);
node * redistribute_nodes(node * root, node * n, node * neighbor,
                          int neighbor_index,
		int k_prime_index, int k_prime);
node * delete_entry( node * root, node * n, int key, void * pointer );
node * delete( node * root, int key );

void getTreeDataRecords(treeRoot* root, dataRecord** oRecords);

int find_result_indices_scan_unclustered_select(treeRoot * root, int key_start, int key_end, int* resultIndices);
int find_lower_index_clustered(treeRoot* root, long int lowVal);
int find_higher_index_clustered(treeRoot* root, long int highVal);
void destroy_tree_nodes(void* node);
void destroy_tree(treeRoot * root);

void enqueue( void* new_node, bool isLeaf);
void* dequeue( void );
void writedata(void* node, FILE* fp);
void write_tree_to_file(treeRoot* root, FILE* fp);
void readdata(void* node, void* prevNode, int levels, FILE* fp);
treeRoot* read_tree_from_file(FILE* fp);
leafNode* find_leafN(treeRoot* root, int key);
int findN(treeRoot* root, int key);
treeRoot* insert_in_treeN(treeRoot* root, int key, int pos );
treeRoot* start_new_treeN(treeRoot* root, int key, int pos);
leafNode* make_leafN();
internalNode* make_nodeN();
leafNode* insert_into_leafN(leafNode* leaf, int key, int pos);
treeRoot* insert_into_leaf_after_splittingN(treeRoot* root, leafNode* leaf, int key, int pos);
treeRoot * insert_into_parent_of_leaf_nodeN(treeRoot* root, leafNode* left, int key, leafNode* right);
treeRoot * insert_into_parent_of_internal_nodeN(treeRoot* root, internalNode* left, int key, internalNode* right);
internalNode* insert_into_new_rootN(void* left, int key, void* right, bool isLeaf);
int get_left_indexN(internalNode* parent, void* left);
treeRoot* insert_into_nodeN(treeRoot* root, internalNode* parent, int left_index, int key, void* right);
treeRoot* insert_into_node_after_splittingN(treeRoot * root, internalNode* old_node, int left_index, int key, void* right);

#endif
