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

int cutPosition( int length );
void getTreeDataRecords(treeRoot* root, dataRecord** oRecords);
int find_result_indices_scan_unclustered_select(treeRoot * root, int key_start, int key_end, int* resultIndices);
int find_lower_index_clustered(treeRoot* root, long int lowVal);
int find_higher_index_clustered(treeRoot* root, long int highVal);
void destroy_tree_nodes(void* node);
void destroy_tree(treeRoot * root);
void writedata(void* node, FILE* fp);
void write_tree_to_file(treeRoot* root, FILE* fp);
void readdata(void* node, void* prevNode, int levels, FILE* fp, leafNode** pLeafConnection);
treeRoot* read_tree_from_file(FILE* fp);
leafNode* find_leaf_node(treeRoot* root, int key);
int find_node(treeRoot* root, int key);
treeRoot* insert_key_in_tree(treeRoot* root, int key, int pos );
treeRoot* create_new_tree(treeRoot* root, int key, int pos);
leafNode* create_leaf_node();
internalNode* create_internal_node();
leafNode* insert_into_leaf(leafNode* leaf, int key, int pos);
treeRoot* insert_into_leaf_after_splitting(treeRoot* root, leafNode* leaf, int key, int pos);
treeRoot * insert_into_parent_of_leaf_node(treeRoot* root, leafNode* left, int key, leafNode* right);
treeRoot * insert_into_parent_of_internal_node(treeRoot* root, internalNode* left, int key, internalNode* right);
internalNode* insert_into_new_root(void* left, int key, void* right, bool isLeaf);
int get_left_index(internalNode* parent, void* left);
treeRoot* insert_into_node(treeRoot* root, internalNode* parent, int left_index, int key, void* right);
treeRoot* insert_into_node_after_splitting(treeRoot * root, internalNode* old_node, int left_index, int key, void* right);
treeRoot* delete_key(treeRoot* root, int key);
treeRoot* adjust_root(treeRoot* root) ;
leafNode* remove_entry_from_node(leafNode* n, int key, int pos);
void find_and_update_position(treeRoot* root, int key, int newPos);

#endif
