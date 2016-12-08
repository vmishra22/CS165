//This Btree implementation has been implemented with the suggestions from the following sources:
//http://www.cs.yale.edu/homes/aspnes/pinewiki/BTrees.html
//https://stuff.mit.edu/afs/sipb/user/gamadrid/nscript/btree.c
//http://www.amittai.com/prose/bpt.c
//https://inst.eecs.berkeley.edu/~cs186/fa04/btree_html/

#include "btree.h"
#include <string.h>
#include <unistd.h>
#include <stdint.h>


int order = 257;
void* queue = NULL;
bool verbose_output = false;

void getTreeDataRecords(treeRoot* root, dataRecord** oRecords){
	int i = 0;int index = 0;
	if(root == NULL || (root->iNode == NULL && root->lNode == NULL))
		return;
	leafNode* lNode = NULL;
	if(root->isLeaf){
		lNode = root->lNode;
		for (i = 0; i < lNode->num_keys; i++) {
			dataRecord* pDataRecord = &((*oRecords)[index++]);
			pDataRecord->val = lNode->keys[i];
			pDataRecord->pos = lNode->pos[i];
		}
	}
	else{
		void* iNode = root->iNode;
		int numLevels = 0;

		while (root->levels > numLevels) {
			iNode = ((internalNode*)iNode)->pointers[0];
			numLevels++;
		}
		lNode = (leafNode*)iNode;
		while (true) {
			for (i = 0; i < lNode->num_keys; i++) {
				dataRecord* pDataRecord = &((*oRecords)[index++]);
				pDataRecord->val = lNode->keys[i];
				pDataRecord->pos = lNode->pos[i];
			}
			if (lNode->nextLeafNode != NULL) {
				lNode = lNode->nextLeafNode;
			}
			else
				break;
		}
	}
}

void readdata(void* node, void* prevNode, int levels, FILE* fp, leafNode** pLeafConnection){
	int i = 0;
	if(node == NULL)
		return;

	bool isLeafNode = false;
	if(((leafNode*)node)->isLeaf)
		isLeafNode=true;

	size_t readval = 0;
	if(isLeafNode)
	{
		leafNode* lNode = (leafNode*)node;
		readval = fread(&(lNode->isLeaf), sizeof(bool), 1, fp);
		readval = fread(&(lNode->num_keys), sizeof(int), 1, fp);
		readval = fread(&(lNode->node_id), sizeof(uuid_t), 1, fp);
		long nextNodeOffset = 0;
		readval = fread(&(nextNodeOffset), sizeof(long), 1, fp);
		lNode->nextLeafNode = NULL;
		
		long parentNodeOffset = 0;
		readval = fread(&(parentNodeOffset), sizeof(long), 1, fp);
		if(prevNode != NULL)
			lNode->parent = prevNode;
	
		readval = fread(lNode->keys, sizeof(int), lNode->num_keys, fp );
		readval = fread(lNode->pos, sizeof(int), lNode->num_keys, fp );
		(void)readval;
	}else{
		internalNode* iNode = (internalNode*)node;
		readval = fread(&(iNode->isLeaf), sizeof(bool), 1, fp);
		readval = fread(&(iNode->num_keys), sizeof(int), 1, fp);
		readval = fread(&(iNode->node_id), sizeof(uuid_t), 1, fp);
		long parentNodeOffset = 0;
		readval = fread(&(parentNodeOffset), sizeof(long), 1, fp);
		if(prevNode != NULL)
			iNode->parent = prevNode;
		
		bool childLeafNodes = false;
		if(levels-1 == 0)
			childLeafNodes = true;

		readval = fread(iNode->keys, sizeof(int), iNode->num_keys, fp );
		if(childLeafNodes){
			for (i = 0; i <= iNode->num_keys; i++){
				long childNodeOffset = 0;
				readval = fread(&(childNodeOffset), sizeof(long), 1, fp);
				long curr_pos = ftell(fp);
				leafNode* lChildNode = create_leaf_node();
				iNode->pointers[i] = lChildNode;
				fseek(fp, childNodeOffset, SEEK_SET);
				readdata(lChildNode, iNode, levels-1, fp, pLeafConnection);
				fseek(fp, curr_pos, SEEK_SET);
			}
			if(*pLeafConnection != NULL){
				leafNode* lChildNode = (leafNode*)(iNode->pointers[0]);
				(*pLeafConnection)->nextLeafNode = lChildNode;
			}
			for (i = 0; i <= iNode->num_keys; i++){
				leafNode* lChildNodePrev = iNode->pointers[i];
				if(lChildNodePrev == NULL || (i==iNode->num_keys))
					break;
				leafNode* lChildNodeNext = iNode->pointers[i+1];
				if(lChildNodeNext != NULL){
					lChildNodePrev->nextLeafNode = lChildNodeNext;
					*pLeafConnection = lChildNodeNext;
				}
				else{
					lChildNodePrev->nextLeafNode = NULL;
					break;
				}
			}
		}else{
			for (i = 0; i <= iNode->num_keys; i++){
				long childNodeOffset = 0;
				readval = fread(&(childNodeOffset), sizeof(long), 1, fp);
				long curr_pos = ftell(fp);
				internalNode* iChildNode = create_internal_node();
				iNode->pointers[i] = iChildNode;
				fseek(fp, childNodeOffset, SEEK_SET);
				readdata(iChildNode, iNode, levels-1, fp, pLeafConnection);
				fseek(fp, curr_pos, SEEK_SET);
			}
		}
	}
	(void)readval;
}

treeRoot* read_tree_from_file(FILE* fp){
	if(fp == NULL)
		return NULL;

	treeRoot* root = (treeRoot*)malloc(sizeof(treeRoot));
	memset(root, 0, sizeof(treeRoot));

	size_t readval = 0;
	long rootNodeOffset = 0;
	readval = fread(&(root->isLeaf), sizeof(bool), 1, fp);
	readval = fread(&(root->levels), sizeof(int), 1, fp);
	readval = fread(&(root->num_nodes), sizeof(int), 1, fp);

	leafNode* leafConnection = NULL;
	if(root->isLeaf){
		readval = fread(&(rootNodeOffset), sizeof(long), 1, fp);
		fseek(fp, rootNodeOffset, SEEK_SET);
		leafNode* lNode = create_leaf_node();
		root->lNode = lNode;
		readdata(root->lNode, NULL, root->levels, fp, &leafConnection);
	}else{
		readval = fread(&(rootNodeOffset), sizeof(long), 1, fp);
		fseek(fp, rootNodeOffset, SEEK_SET);
		internalNode* iNode = create_internal_node();
		root->iNode = iNode;
		readdata(root->iNode, NULL, root->levels, fp, &leafConnection);
	}

	(void)readval;
	return root;
}

void writedata(void* node, FILE* fp){
	int i = 0;
	if(node == NULL)
		return;
	bool isLeafNode = false;
	if(((leafNode*)node)->isLeaf)
		isLeafNode=true;
	if(isLeafNode)
	{
		leafNode* lNode = (leafNode*)node;
		fwrite(&(lNode->isLeaf), sizeof(bool), 1, fp);
		fwrite(&(lNode->num_keys), sizeof(int), 1, fp);
		fwrite(&(lNode->node_id), sizeof(uuid_t), 1, fp);
		uintptr_t nextNodeOffset = 0;
		if(lNode->nextLeafNode != NULL)
			nextNodeOffset = (uintptr_t) (lNode->nextLeafNode);
		fwrite(&(nextNodeOffset), sizeof(long), 1, fp);
		uintptr_t parentNodeOffset = 0;
		if(lNode->parent != NULL)
			parentNodeOffset = (uintptr_t) (lNode->parent);
		fwrite(&(parentNodeOffset), sizeof(long), 1, fp);
		fwrite(lNode->keys, sizeof(int), lNode->num_keys, fp );
		fwrite(lNode->pos, sizeof(int), lNode->num_keys, fp );
	}else{
		internalNode* iNode = (internalNode*)node;
		fwrite(&(iNode->isLeaf), sizeof(bool), 1, fp);
		fwrite(&(iNode->num_keys), sizeof(int), 1, fp);
		fwrite(&(iNode->node_id), sizeof(uuid_t), 1, fp);
		uintptr_t parentNodeOffset = 0;
		if(iNode->parent != NULL)
			parentNodeOffset = (uintptr_t) (iNode->parent);
		fwrite(&(parentNodeOffset), sizeof(long), 1, fp);
		fwrite(iNode->keys, sizeof(int), iNode->num_keys, fp );
		for (i = 0; i <= iNode->num_keys; i++){
			void* childPointer = iNode->pointers[i];
			uintptr_t childNodeOffset = (uintptr_t)childPointer;
			fwrite(&(childNodeOffset), sizeof(uintptr_t), 1, fp);
			long curr_pos = ftell(fp);
			fseek(fp, childNodeOffset, SEEK_SET);
			writedata(childPointer, fp);
			fseek(fp, curr_pos, SEEK_SET);
		}
	}
}

void write_tree_to_file(treeRoot* root, FILE* fp) {
	if (root == NULL) {
		return;
	}
	uintptr_t rootNodeOffset = 0;
	
	fwrite(&(root->isLeaf), sizeof(bool), 1, fp);
	fwrite(&(root->levels), sizeof(int), 1, fp);
	fwrite(&(root->num_nodes), sizeof(int), 1, fp);

	if(root->isLeaf){
		rootNodeOffset = (uintptr_t)(root->lNode);
		fwrite(&(rootNodeOffset), sizeof(long), 1, fp);
		fseek(fp, rootNodeOffset, SEEK_SET);
		writedata(root->lNode, fp);
	}else{
		rootNodeOffset = (uintptr_t)(root->iNode);
		fwrite(&(rootNodeOffset), sizeof(long), 1, fp);
		fseek(fp, rootNodeOffset, SEEK_SET);
		writedata(root->iNode, fp);
	}
}

//This function returns the index of searched key
//relative to the the first leaf node of the tree. Root can't be a leaf node.
int get_relative_range_index(treeRoot* root, leafNode* targetLeafNode){
	void* iNode = root->iNode;
	int numLevels = 0;

	while (root->levels > numLevels) {
		iNode = ((internalNode*)iNode)->pointers[0];
		numLevels++;
	}
	leafNode* lNode = (leafNode*)iNode; //This is the first leaf of tree
	int nKeys = 0;
	while(lNode != targetLeafNode){
		nKeys += (lNode->num_keys);
		lNode = lNode->nextLeafNode;
	}

	return nKeys;
}
int find_lower_index_clustered(treeRoot* root, long int lowVal){
	int i;
	leafNode* lNode = find_leaf_node(root, lowVal);
	if (lNode == NULL) return 0;
	for (i = 0; i < lNode->num_keys && lNode->keys[i] < lowVal; i++) ;
	if (i == lNode->num_keys) return 0;
	
	if(!(root->isLeaf)){
		int num_indices = get_relative_range_index(root, lNode);
		i += num_indices;
	}

	return i;
}

int find_higher_index_clustered(treeRoot* root, long int highVal){
	int i;
	leafNode* lNode = find_leaf_node(root, highVal);
	if (lNode == NULL) return 0;
	for (i = 0; i < lNode->num_keys && lNode->keys[i] < highVal; i++) ;
	if (i == lNode->num_keys) return 0;

	if(!(root->isLeaf)){
		int num_indices = get_relative_range_index(root, lNode);
		i += num_indices;
	}
	if(i>0) i--;
	return i;
}

int find_result_indices_scan_unclustered_select(treeRoot* root, int key_start, int key_end, int* resultIndices){
	int i, num_found;
	num_found = 0;
	leafNode* lNode = find_leaf_node(root, key_start);
	if (lNode == NULL) return 0;
	for (i = 0; i < lNode->num_keys && lNode->keys[i] < key_start; i++) ;
	if (i == lNode->num_keys) return 0;
	while (lNode != NULL) {
		for ( ; i < lNode->num_keys && lNode->keys[i] < key_end; i++) {
			int pos = lNode->pos[i];
			resultIndices[num_found++] = pos;
		}
		lNode = lNode->nextLeafNode;
		i = 0;
	}
	return num_found;
}

leafNode* find_leaf_node(treeRoot* root, int key){
	if(root == NULL || (root->iNode == NULL && root->lNode == NULL))
		return NULL;
	leafNode* lNode = NULL;
	int i = 0;
	if(root->isLeaf)
		return (root->lNode);
	else{
		void* iNode = root->iNode;
		int numLevels = 0;

		while (root->levels > numLevels) {
			i = 0;
			//Binary search can be used here instead of scan. 
			while (i < ((internalNode*)iNode)->num_keys) {
				if (key >= ((internalNode*)iNode)->keys[i]) i++;
				else break;
			}
			
			iNode = ((internalNode*)iNode)->pointers[i];
			numLevels++;
		}
		lNode = (leafNode*)iNode;
	}

	return lNode;
}

//return the position of column base data for a specific key.
int find_node(treeRoot* root, int key){
	int i = 0;
	leafNode* lNode = find_leaf_node(root, key);
	if (lNode == NULL) return -1;
	for (i = 0; i < lNode->num_keys; i++)
		if (lNode->keys[i] == key) break;
	if (i == lNode->num_keys) 
		return -1;
	else
		return lNode->pos[i];

	return 0;
}

void find_and_update_position(treeRoot* root, int key, int newPos){
	int i = 0;
	leafNode* lNode = find_leaf_node(root, key);
	if (lNode == NULL) return;
	for (i = 0; i < lNode->num_keys; i++)
		if (lNode->keys[i] == key) break;
	if (i == lNode->num_keys) 
		return;
	else
		lNode->pos[i] = newPos;

	return;
}

int cutPosition(int length ) {
	if (length % 2 == 0)
		return length/2;
	else
		return length/2 + 1;
}

//make a new internal node
internalNode* create_internal_node() {
	size_t system_page_size = sysconf(_SC_PAGESIZE);
	internalNode* iNode = (internalNode*)malloc(system_page_size);
	memset(iNode, 0xCC, system_page_size);
	iNode->isLeaf = false;
	iNode->num_keys = 0;
	uuid_generate(iNode->node_id);
	iNode->parent = NULL;
	iNode->keys = (int*)((unsigned char*)iNode + (64));
	memset(iNode->keys, -1, sizeof(int)*(MAX_ORDER - 1));
	iNode->pointers = (void**)((unsigned char*)(iNode->keys) + (MAX_ORDER-1)*sizeof(int));
	memset(iNode->pointers, 0, sizeof(void*)*(MAX_ORDER));
	return iNode;
}

//Make a brand new leaf node. 
leafNode* create_leaf_node() {
	size_t system_page_size = sysconf(_SC_PAGESIZE);
	leafNode* lNode = (leafNode*)malloc(system_page_size);
	memset(lNode, 0xCC, system_page_size);
	lNode->isLeaf = true;
	lNode->num_keys = 0;
	uuid_generate(lNode->node_id);
	lNode->nextLeafNode = NULL;
	lNode->parent = NULL;
	lNode->keys = (int*)((unsigned char*)lNode + (64));
	memset(lNode->keys, -1, sizeof(int)*(MAX_ORDER - 1));
	lNode->pos = (int*)((unsigned char*)(lNode->keys) + (MAX_ORDER - 1)*sizeof(int));
	memset(lNode->pos, -1, sizeof(int)*(MAX_ORDER - 1));
	return lNode;
}

int get_left_index(internalNode* parent, void* left) {
	int left_index = 0;
	while (left_index <= parent->num_keys && 
			parent->pointers[left_index] != left)
		left_index++;
	return left_index;
}

leafNode* insert_into_leaf(leafNode* leaf, int key, int pos) {

	int i, insertion_point;

	insertion_point = 0;
	while (insertion_point < leaf->num_keys && leaf->keys[insertion_point] < key)
		insertion_point++;

	for (i = leaf->num_keys; i > insertion_point; i--) {
		leaf->keys[i] = leaf->keys[i - 1];
		leaf->pos[i] = leaf->pos[i - 1];
	}
	leaf->keys[insertion_point] = key;
	leaf->pos[insertion_point] = pos;
	leaf->num_keys++;
	return leaf;
}

treeRoot* insert_into_leaf_after_splitting(treeRoot* root, leafNode* leaf, int key, int pos) {

	leafNode* new_leaf;
	int* temp_keys;
	int* temp_pos;
	int insertion_index, split, new_key, i, j;

	new_leaf = create_leaf_node();

	temp_keys = malloc(order * sizeof(int));
	if (temp_keys == NULL) {
		perror("Temporary keys array.");
		exit(EXIT_FAILURE);
	}

	temp_pos = malloc( order * sizeof(int) );
	if (temp_pos == NULL) {
		perror("Temporary positions array.");
		exit(EXIT_FAILURE);
	}

	insertion_index = 0;
	while (insertion_index < order - 1 && leaf->keys[insertion_index] < key)
		insertion_index++;

	for (i = 0, j = 0; i < leaf->num_keys; i++, j++) {
		if (j == insertion_index) j++;
		temp_keys[j] = leaf->keys[i];
		temp_pos[j] = leaf->pos[i];
	}

	temp_keys[insertion_index] = key;
	temp_pos[insertion_index] = pos;

	leaf->num_keys = 0;

	split = cutPosition(order - 1);

	for (i = 0; i < split; i++) {
		leaf->pos[i] = temp_pos[i];
		leaf->keys[i] = temp_keys[i];
		leaf->num_keys++;
	}

	for (i = split, j = 0; i < order; i++, j++) {
		new_leaf->pos[j] = temp_pos[i];
		new_leaf->keys[j] = temp_keys[i];
		new_leaf->num_keys++;
	}

	free(temp_pos);
	free(temp_keys);

	new_leaf->nextLeafNode = leaf->nextLeafNode;
	leaf->nextLeafNode = new_leaf;

	for (i = leaf->num_keys; i < order - 1; i++)
		leaf->pos[i] = -1;
	for (i = new_leaf->num_keys; i < order - 1; i++)
		new_leaf->pos[i] = -1;

	new_leaf->parent = leaf->parent;
	new_key = new_leaf->keys[0];

	return insert_into_parent_of_leaf_node(root, leaf, new_key, new_leaf);
}


treeRoot* insert_into_node(treeRoot* root, internalNode* parent, int left_index, int key, void* right) {
	int i;
	for (i = parent->num_keys; i > left_index; i--) {
		parent->pointers[i + 1] = parent->pointers[i];
		parent->keys[i] = parent->keys[i - 1];
	}
	parent->pointers[left_index + 1] = right;
	parent->keys[left_index] = key;
	parent->num_keys++;
	return root;
}


treeRoot* insert_into_node_after_splitting(treeRoot * root, internalNode* old_node, int left_index, int key, void* right) {

	int i, j, split, k_prime;
	internalNode* new_node;
	int * temp_keys;
	void ** temp_pointers;
	void* child;

	/* First create a temporary set of keys and pointers
	 * to hold everything in order, including
	 * the new key and pointer, inserted in their
	 * correct places. 
	 * Then create a new node and copy half of the 
	 * keys and pointers to the old node and
	 * the other half to the new.
	 */

	temp_pointers = (void**)malloc( (order + 1) * sizeof(void*) );
	if (temp_pointers == NULL) {
		perror("Temporary pointers array for splitting nodes.");
		exit(EXIT_FAILURE);
	}
	temp_keys = (int*)malloc( order * sizeof(int) );
	if (temp_keys == NULL) {
		perror("Temporary keys array for splitting nodes.");
		exit(EXIT_FAILURE);
	}

	for (i = 0, j = 0; i < old_node->num_keys + 1; i++, j++) {
		if (j == left_index + 1) j++;
		temp_pointers[j] = (old_node->pointers[i]);
	}

	for (i = 0, j = 0; i < old_node->num_keys; i++, j++) {
		if (j == left_index) j++;
		temp_keys[j] = old_node->keys[i];
	}

	temp_pointers[left_index + 1] = right;
	temp_keys[left_index] = key;

	/* Create the new node and copy
	 * half the keys and pointers to the
	 * old and half to the new.
	 */  
	split = cutPosition(order);
	new_node = create_internal_node();
	old_node->num_keys = 0;
	for (i = 0; i < split - 1; i++) {
		old_node->pointers[i] = temp_pointers[i];
		old_node->keys[i] = temp_keys[i];
		old_node->num_keys++;
	}
	old_node->pointers[i] = temp_pointers[i];
	k_prime = temp_keys[split - 1];
	for (++i, j = 0; i < order; i++, j++) {
		new_node->pointers[j] = temp_pointers[i];
		new_node->keys[j] = temp_keys[i];
		new_node->num_keys++;
	}
	new_node->pointers[j] = temp_pointers[i];
	free(temp_pointers);
	free(temp_keys);
	new_node->parent = old_node->parent;
	for (i = 0; i <= new_node->num_keys; i++) {
		bool isLeafNode = false;
		child = (new_node->pointers[i]);
		if(((leafNode*)child)->isLeaf)
			isLeafNode=true;
		if(isLeafNode)
		{
			leafNode* childlNode = (leafNode*)child;
			childlNode->parent = new_node;
		}else{
			internalNode* childiNode = (internalNode*)child;
			childiNode->parent = new_node;
		}
	}

	/* Insert a new key into the parent of the two
	 * nodes resulting from the split, with
	 * the old node to the left and the new to the right.
	 */

	return insert_into_parent_of_internal_node(root, old_node, k_prime, new_node);
}

treeRoot * insert_into_parent_of_internal_node(treeRoot* root, internalNode* left, int key, internalNode* right) {

	int left_index;
	internalNode* parent;

	parent = left->parent;

	/* Case: new root. */

	if (parent == NULL){
		internalNode* newRootNode = insert_into_new_root(left, key, right, false);
		root->iNode = newRootNode;
		root->lNode = NULL;
		root->levels++;
		root->isLeaf = false;
		return root;
	}

	/* Case: leaf or node. (Remainder of
	 * function body.)  
	 */

	/* Find the parent's pointer to the left 
	 * node.
	 */

	left_index = get_left_index(parent, left);


	/* Simple case: the new key fits into the node. 
	 */

	if (parent->num_keys < order - 1)
		return insert_into_node(root, parent, left_index, key, right);

	/* Harder case:  split a node in order 
	 * to preserve the B+ tree properties.
	 */

	return insert_into_node_after_splitting(root, parent, left_index, key, right);
}

treeRoot * insert_into_parent_of_leaf_node(treeRoot* root, leafNode* left, int key, leafNode* right) {

	int left_index;
	internalNode* parent;

	parent = left->parent;

	/* Case: new root. */
	if (parent == NULL)
	{
		internalNode* newRootNode = insert_into_new_root(left, key, right, true);
		root->iNode = newRootNode;
		root->lNode = NULL;
		root->levels++;
		root->isLeaf = false;
		return root;
	}

	/* Case: leaf or node. (Remainder of function body.)  
	 */
	/* Find the parent's pointer to the left node.
	 */
	left_index = get_left_index(parent, left);

	/* Simple case: the new key fits into the node. 
	 */
	if (parent->num_keys < order - 1)
		return insert_into_node(root, parent, left_index, key, right);

	/* Harder case:  split a node in order 
	 * to preserve the B+ tree properties.
	 */

	return insert_into_node_after_splitting(root, parent, left_index, key, right);
}


internalNode* insert_into_new_root(void* left, int key, void* right, bool isLeaf) {
	internalNode* root = create_internal_node();
	root->keys[0] = key;
	root->pointers[0] = left;
	root->pointers[1] = right;
	root->num_keys++;
	root->parent = NULL;
	if(isLeaf){
		((leafNode*)left)->parent = root;
		((leafNode*)right)->parent = root;
	}else{
		((internalNode*)left)->parent = root;
		((internalNode*)right)->parent = root;
	}
	return root;
}


//starting a new tree from first key, position pair
treeRoot* create_new_tree(treeRoot* root, int key, int pos) {
	root->lNode = create_leaf_node();
	*(root->lNode->keys) = key;
	root->lNode->pos[0] = pos;
	root->lNode->nextLeafNode = NULL;
	root->lNode->parent = NULL;
	root->lNode->num_keys++;
	root->levels = 0;
	root->isLeaf = true;
	root->num_nodes = 1;
	return root;
}

//Insert the key and position into the tree
treeRoot* insert_key_in_tree(treeRoot* root, int key, int pos ) {
	//root structure needs to be allocated first.
	if(root == NULL)
		return NULL;

	//No duplicates for now in the tree. 
	// if(find_node(root, key) != -1)
	// 	return root;

	//Create a new tree for the first time. 
	if (root->iNode == NULL && root->lNode == NULL) 
		return create_new_tree(root, key, pos);


	/* Case: the tree already exists.
	 * (Rest of function body.)
	 */

	leafNode* leaf = find_leaf_node(root, key);

	/* Case: leaf has room for key and pointer.
	*/

	if (leaf->num_keys < order - 1) {
		leaf = insert_into_leaf(leaf, key, pos);
		return root;
	}


	// /* Case:  leaf must be split.
	//  */

	return insert_into_leaf_after_splitting(root, leaf, key, pos);
}


// DELETION.

leafNode* remove_entry_from_node(leafNode* n, int key, int pos) {

	int i;

	// Remove the key and shift other keys accordingly.
	i = 0;
	while (n->keys[i] != key)
		i++;
	for (++i; i < n->num_keys; i++)
		n->keys[i - 1] = n->keys[i];

	while (n->pos[i] != pos)
		i++;
	for (++i; i < n->num_keys; i++)
		n->pos[i - 1] = n->pos[i];


	// One key fewer.
	n->num_keys--;

	return n;
}


treeRoot* adjust_root(treeRoot* root) {

	treeRoot* new_root;

	internalNode* iNode = NULL;
	leafNode* lNode = NULL;

	if(root->isLeaf){
		lNode = root->lNode;
		if(lNode->num_keys > 0)
			return root;
	}else{
		iNode = root->iNode;
		if(iNode->num_keys > 0)
			return root;
	}

	if (!root->isLeaf) {
		new_root = malloc(sizeof(treeRoot));
		new_root->iNode = iNode->pointers[0];
		new_root->iNode->parent = NULL;
	}else
		new_root = NULL;

	if(root->isLeaf){
		free(root->lNode);
		free(root);
	}else{
		free(root->iNode);
		free(root);
	}
	
	return new_root;
}

treeRoot * delete_entry( treeRoot* root, leafNode * n, int key, int pos) {

	// Remove key and pointer from node.

	n = remove_entry_from_node(n, key, pos);

	/* Case:  deletion from the root. 
	 */

	if (n == root->lNode) 
		return adjust_root(root);

	return root;
}

//deletion.
treeRoot* delete_key(treeRoot* root, int key){
	int base_pos = find_node(root, key);
	leafNode* leaf_node = find_leaf_node(root, key);
	if (leaf_node != NULL) {
		root = delete_entry(root, leaf_node, key, base_pos);
	}
	return root;
}

void destroy_tree_nodes(void* node) {
	int i;

	bool isLeafNode = false;
	if(((leafNode*)node)->isLeaf)
		isLeafNode=true;
	
	if(isLeafNode)
	{
		leafNode* lNode = (leafNode*)node;
		free(lNode);
		lNode = NULL;
	}else{
		internalNode* iNode = (internalNode*)node;
		for (i = 0; i <= iNode->num_keys; i++)
			destroy_tree_nodes(iNode->pointers[i]);
		free(iNode);
		iNode = NULL;
	}
}

void destroy_tree(treeRoot * root) {
	if(root->isLeaf){
		free(root->lNode);
		free(root);
		return;
	}
	else{
		destroy_tree_nodes(root->iNode);
		free(root);
	}
	return;
}

