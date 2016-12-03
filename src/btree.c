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

/* Prints the bottom row of keys
 * of the tree (with their respective
 * pointers, if the verbose_output flag is set.
 */
void print_leaves( node * root ) {
	int i;
	node * c = root;
	if (root == NULL) {
		printf("Empty tree.\n");
		return;
	}
	while (!c->is_leaf)
		c = c->pointers[0];
	while (true) {
		for (i = 0; i < c->num_keys; i++) {
			if (verbose_output)
				printf("%lx ", (unsigned long)c->pointers[i]);
			printf("%d ", c->keys[i]);
		}
		if (verbose_output)
			printf("%lx ", (unsigned long)c->pointers[order - 1]);
		if (c->pointers[order - 1] != NULL) {
			printf(" | ");
			c = c->pointers[order - 1];
		}
		else
			break;
	}
	printf("\n");
}


/* Utility function to give the height
 * of the tree, which length in number of edges
 * of the path from the root to any leaf.
 */
int height( node * root ) {
	int h = 0;
	node * c = root;
	while (!c->is_leaf) {
		c = c->pointers[0];
		h++;
	}
	return h;
}


/* Utility function to give the length in edges
 * of the path from any node to the root.
 */
int path_to_root( node * root, node * child ) {
	int length = 0;
	node * c = child;
	while (c != root) {
		c = c->parent;
		length++;
	}
	return length;
}


void readdata(void* node, void* prevNode, int levels, FILE* fp){
	int i = 0;
	if(node == NULL)
		return;

	bool isLeafNode = false;
	if(((leafNode*)node)->isLeaf)
		isLeafNode=true;

	if(isLeafNode)
	{
		leafNode* lNode = (leafNode*)node;
		fread(&(lNode->isLeaf), sizeof(bool), 1, fp);
		fread(&(lNode->num_keys), sizeof(int), 1, fp);
		fread(&(lNode->node_id), sizeof(uuid_t), 1, fp);
		long nextNodeOffset = 0;
		fread(&(nextNodeOffset), sizeof(long), 1, fp);
		lNode->nextLeafNode = NULL;
		
		long parentNodeOffset = 0;
		fread(&(parentNodeOffset), sizeof(long), 1, fp);
		if(prevNode != NULL)
			lNode->parent = prevNode;
	
		fread(lNode->keys, sizeof(int), lNode->num_keys, fp );
		fread(lNode->pos, sizeof(int), lNode->num_keys, fp );
	}else{
		internalNode* iNode = (internalNode*)node;
		fread(&(iNode->isLeaf), sizeof(bool), 1, fp);
		fread(&(iNode->num_keys), sizeof(int), 1, fp);
		fread(&(iNode->node_id), sizeof(uuid_t), 1, fp);
		long parentNodeOffset = 0;
		fread(&(parentNodeOffset), sizeof(long), 1, fp);
		if(prevNode != NULL)
			iNode->parent = prevNode;
		
		bool childLeafNodes = false;
		if(levels-1 == 0)
			childLeafNodes = true;

		fread(iNode->keys, sizeof(int), iNode->num_keys, fp );
		if(childLeafNodes){
			for (i = 0; i <= iNode->num_keys; i++){
				long childNodeOffset = 0;
				fread(&(childNodeOffset), sizeof(long), 1, fp);
				long curr_pos = ftell(fp);
				leafNode* lChildNode = make_leafN();
				iNode->pointers[i] = lChildNode;
				fseek(fp, childNodeOffset, SEEK_SET);
				readdata(lChildNode, iNode, levels-1, fp);
				fseek(fp, curr_pos, SEEK_SET);
			}
			for (i = 0; i <= iNode->num_keys; i++){
				leafNode* lChildNodePrev = iNode->pointers[i];
				if(lChildNodePrev == NULL || (i==iNode->num_keys))
					break;
				leafNode* lChildNodeNext = iNode->pointers[i+1];
				if(lChildNodeNext != NULL){
					lChildNodePrev->nextLeafNode = lChildNodeNext;
				}
				else{
					lChildNodePrev->nextLeafNode = NULL;
					break;
				}
			}
		}else{
			for (i = 0; i <= iNode->num_keys; i++){
				long childNodeOffset = 0;
				fread(&(childNodeOffset), sizeof(long), 1, fp);
				long curr_pos = ftell(fp);
				internalNode* iChildNode = make_nodeN();
				iNode->pointers[i] = iChildNode;
				fseek(fp, childNodeOffset, SEEK_SET);
				readdata(iChildNode, iNode, levels-1, fp);
				fseek(fp, curr_pos, SEEK_SET);
			}
		}
	}
}

treeRoot* read_tree_from_file(FILE* fp){
	if(fp == NULL)
		return NULL;

	treeRoot* root = (treeRoot*)malloc(sizeof(treeRoot));
	memset(root, 0, sizeof(treeRoot));

	long rootNodeOffset = 0;
	fread(&(root->isLeaf), sizeof(bool), 1, fp);
	fread(&(root->levels), sizeof(int), 1, fp);
	fread(&(root->num_nodes), sizeof(int), 1, fp);

	if(root->isLeaf){
		fread(&(rootNodeOffset), sizeof(long), 1, fp);
		fseek(fp, rootNodeOffset, SEEK_SET);
		leafNode* lNode = make_leafN();
		root->lNode = lNode;
		readdata(root->lNode, NULL, root->levels, fp);
	}else{
		fread(&(rootNodeOffset), sizeof(long), 1, fp);
		fseek(fp, rootNodeOffset, SEEK_SET);
		internalNode* iNode = make_nodeN();
		root->iNode = iNode;
		readdata(root->iNode, NULL, root->levels, fp);
		
	}

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
	leafNode* lNode = find_leafN(root, lowVal);
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
	leafNode* lNode = find_leafN(root, highVal);
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
	leafNode* lNode = find_leafN(root, key_start);
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

leafNode* find_leafN(treeRoot* root, int key){
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
int findN(treeRoot* root, int key){
	int i = 0;
	leafNode* lNode = find_leafN(root, key);
	if (lNode == NULL) return -1;
	for (i = 0; i < lNode->num_keys; i++)
		if (lNode->keys[i] == key) break;
	if (i == lNode->num_keys) 
		return -1;
	else
		return lNode->pos[i];

	return 0;
}


/* Finds the appropriate place to
 * split a node that is too big into two.
 */
int cut( int length ) {
	if (length % 2 == 0)
		return length/2;
	else
		return length/2 + 1;
}


// INSERTION
//make a new internal node
internalNode* make_nodeN() {
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
leafNode* make_leafN() {
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

int get_left_indexN(internalNode* parent, void* left) {
	int left_index = 0;
	while (left_index <= parent->num_keys && 
			parent->pointers[left_index] != left)
		left_index++;
	return left_index;
}

leafNode* insert_into_leafN(leafNode* leaf, int key, int pos) {

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

treeRoot* insert_into_leaf_after_splittingN(treeRoot* root, leafNode* leaf, int key, int pos) {

	leafNode* new_leaf;
	int* temp_keys;
	int* temp_pos;
	int insertion_index, split, new_key, i, j;

	new_leaf = make_leafN();

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

	split = cut(order - 1);

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

	return insert_into_parent_of_leaf_nodeN(root, leaf, new_key, new_leaf);
}


treeRoot* insert_into_nodeN(treeRoot* root, internalNode* parent, int left_index, int key, void* right) {
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


treeRoot* insert_into_node_after_splittingN(treeRoot * root, internalNode* old_node, int left_index, int key, void* right) {

	int i, j, split, k_prime;
	internalNode* new_node, * child;
	int * temp_keys;
	void ** temp_pointers;

	/* First create a temporary set of keys and pointers
	 * to hold everything in order, including
	 * the new key and pointer, inserted in their
	 * correct places. 
	 * Then create a new node and copy half of the 
	 * keys and pointers to the old node and
	 * the other half to the new.
	 */

	temp_pointers = malloc( (order + 1) * sizeof(internalNode *) );
	if (temp_pointers == NULL) {
		perror("Temporary pointers array for splitting nodes.");
		exit(EXIT_FAILURE);
	}
	temp_keys = malloc( order * sizeof(int) );
	if (temp_keys == NULL) {
		perror("Temporary keys array for splitting nodes.");
		exit(EXIT_FAILURE);
	}

	for (i = 0, j = 0; i < old_node->num_keys + 1; i++, j++) {
		if (j == left_index + 1) j++;
		temp_pointers[j] = old_node->pointers[i];
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
	split = cut(order);
	new_node = make_nodeN();
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
		child = new_node->pointers[i];
		child->parent = new_node;
	}

	/* Insert a new key into the parent of the two
	 * nodes resulting from the split, with
	 * the old node to the left and the new to the right.
	 */

	return insert_into_parent_of_internal_nodeN(root, old_node, k_prime, new_node);
}

treeRoot * insert_into_parent_of_internal_nodeN(treeRoot* root, internalNode* left, int key, internalNode* right) {

	int left_index;
	internalNode* parent;

	parent = left->parent;

	/* Case: new root. */

	if (parent == NULL){
		internalNode* newRootNode = insert_into_new_rootN(left, key, right, false);
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

	left_index = get_left_indexN(parent, left);


	/* Simple case: the new key fits into the node. 
	 */

	if (parent->num_keys < order - 1)
		return insert_into_nodeN(root, parent, left_index, key, right);

	/* Harder case:  split a node in order 
	 * to preserve the B+ tree properties.
	 */

	return insert_into_node_after_splittingN(root, parent, left_index, key, right);
}

treeRoot * insert_into_parent_of_leaf_nodeN(treeRoot* root, leafNode* left, int key, leafNode* right) {

	int left_index;
	internalNode* parent;

	parent = left->parent;

	/* Case: new root. */
	if (parent == NULL)
	{
		internalNode* newRootNode = insert_into_new_rootN(left, key, right, true);
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
	left_index = get_left_indexN(parent, left);

	/* Simple case: the new key fits into the node. 
	 */
	if (parent->num_keys < order - 1)
		return insert_into_nodeN(root, parent, left_index, key, right);

	/* Harder case:  split a node in order 
	 * to preserve the B+ tree properties.
	 */

	return insert_into_node_after_splittingN(root, parent, left_index, key, right);
}


internalNode* insert_into_new_rootN(void* left, int key, void* right, bool isLeaf) {
	internalNode* root = make_nodeN();
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
treeRoot* start_new_treeN(treeRoot* root, int key, int pos) {
	root->lNode = make_leafN();
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
treeRoot* insert_in_treeN(treeRoot* root, int key, int pos ) {
	//root structure needs to be allocated first.
	if(root == NULL)
		return NULL;

	//No duplicates for now in the tree. 
	// if(findN(root, key) != -1)
	// 	return root;

	//Create a new tree for the first time. 
	if (root->iNode == NULL && root->lNode == NULL) 
		return start_new_treeN(root, key, pos);


	/* Case: the tree already exists.
	 * (Rest of function body.)
	 */

	leafNode* leaf = find_leafN(root, key);

	/* Case: leaf has room for key and pointer.
	*/

	if (leaf->num_keys < order - 1) {
		leaf = insert_into_leafN(leaf, key, pos);
		return root;
	}


	// /* Case:  leaf must be split.
	//  */

	return insert_into_leaf_after_splittingN(root, leaf, key, pos);
}


// DELETION.

/* Utility function for deletion.  Retrieves
 * the index of a node's nearest neighbor (sibling)
 * to the left if one exists.  If not (the node
 * is the leftmost child), returns -1 to signify
 * this special case.
 */
int get_neighbor_index( node * n ) {

	int i;

	/* Return the index of the key to the left
	 * of the pointer in the parent pointing
	 * to n.  
	 * If n is the leftmost child, this means
	 * return -1.
	 */
	for (i = 0; i <= n->parent->num_keys; i++)
		if (n->parent->pointers[i] == n)
			return i - 1;

	// Error state.
	printf("Search for nonexistent pointer to node in parent.\n");
	printf("Node:  %#lx\n", (unsigned long)n);
	exit(EXIT_FAILURE);
}

leafNode* remove_entry_from_nodeN(leafNode* n, int key, int pos) {

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

node * remove_entry_from_node(node * n, int key, node * pointer) {

	int i, num_pointers;

	// Remove the key and shift other keys accordingly.
	i = 0;
	while (n->keys[i] != key)
		i++;
	for (++i; i < n->num_keys; i++)
		n->keys[i - 1] = n->keys[i];

	// Remove the pointer and shift other pointers accordingly.
	// First determine number of pointers.
	num_pointers = n->is_leaf ? n->num_keys : n->num_keys + 1;
	i = 0;
	while (n->pointers[i] != pointer)
		i++;
	for (++i; i < num_pointers; i++)
		n->pointers[i - 1] = n->pointers[i];


	// One key fewer.
	n->num_keys--;

	// Set the other pointers to NULL for tidiness.
	// A leaf uses the last pointer to point to the next leaf.
	if (n->is_leaf)
		for (i = n->num_keys; i < order - 1; i++)
			n->pointers[i] = NULL;
	else
		for (i = n->num_keys + 1; i < order; i++)
			n->pointers[i] = NULL;

	return n;
}

treeRoot* adjust_rootN(treeRoot* root) {

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

node * adjust_root(node * root) {

	node * new_root;

	/* Case: nonempty root.
	 * Key and pointer have already been deleted,
	 * so nothing to be done.
	 */

	if (root->num_keys > 0)
		return root;

	/* Case: empty root. 
	 */

	// If it has a child, promote 
	// the first (only) child
	// as the new root.

	if (!root->is_leaf) {
		new_root = root->pointers[0];
		new_root->parent = NULL;
	}

	// If it is a leaf (has no children),
	// then the whole tree is empty.

	else
		new_root = NULL;

	free(root->keys);
	free(root->pointers);
	free(root);

	return new_root;
}


/* Coalesces a node that has become
 * too small after deletion
 * with a neighboring node that
 * can accept the additional entries
 * without exceeding the maximum.
 */
node * coalesce_nodes(node * root, node * n, node * neighbor, int neighbor_index, int k_prime) {

	int i, j, neighbor_insertion_index, n_end;
	node * tmp;

	/* Swap neighbor with node if node is on the
	 * extreme left and neighbor is to its right.
	 */

	if (neighbor_index == -1) {
		tmp = n;
		n = neighbor;
		neighbor = tmp;
	}

	/* Starting point in the neighbor for copying
	 * keys and pointers from n.
	 * Recall that n and neighbor have swapped places
	 * in the special case of n being a leftmost child.
	 */

	neighbor_insertion_index = neighbor->num_keys;

	/* Case:  nonleaf node.
	 * Append k_prime and the following pointer.
	 * Append all pointers and keys from the neighbor.
	 */

	if (!n->is_leaf) {

		/* Append k_prime.
		 */

		neighbor->keys[neighbor_insertion_index] = k_prime;
		neighbor->num_keys++;


		n_end = n->num_keys;

		for (i = neighbor_insertion_index + 1, j = 0; j < n_end; i++, j++) {
			neighbor->keys[i] = n->keys[j];
			neighbor->pointers[i] = n->pointers[j];
			neighbor->num_keys++;
			n->num_keys--;
		}

		/* The number of pointers is always
		 * one more than the number of keys.
		 */

		neighbor->pointers[i] = n->pointers[j];

		/* All children must now point up to the same parent.
		 */

		for (i = 0; i < neighbor->num_keys + 1; i++) {
			tmp = (node *)neighbor->pointers[i];
			tmp->parent = neighbor;
		}
	}

	/* In a leaf, append the keys and pointers of
	 * n to the neighbor.
	 * Set the neighbor's last pointer to point to
	 * what had been n's right neighbor.
	 */

	else {
		for (i = neighbor_insertion_index, j = 0; j < n->num_keys; i++, j++) {
			neighbor->keys[i] = n->keys[j];
			neighbor->pointers[i] = n->pointers[j];
			neighbor->num_keys++;
		}
		neighbor->pointers[order - 1] = n->pointers[order - 1];
	}

	root = delete_entry(root, n->parent, k_prime, n);
	free(n->keys);
	free(n->pointers);
	free(n); 
	return root;
}


/* Redistributes entries between two nodes when
 * one has become too small after deletion
 * but its neighbor is too big to append the
 * small node's entries without exceeding the
 * maximum
 */
node * redistribute_nodes(node * root, node * n, node * neighbor, int neighbor_index, 
		int k_prime_index, int k_prime) {  

	int i;
	node * tmp;

	/* Case: n has a neighbor to the left. 
	 * Pull the neighbor's last key-pointer pair over
	 * from the neighbor's right end to n's left end.
	 */

	if (neighbor_index != -1) {
		if (!n->is_leaf)
			n->pointers[n->num_keys + 1] = n->pointers[n->num_keys];
		for (i = n->num_keys; i > 0; i--) {
			n->keys[i] = n->keys[i - 1];
			n->pointers[i] = n->pointers[i - 1];
		}
		if (!n->is_leaf) {
			n->pointers[0] = neighbor->pointers[neighbor->num_keys];
			tmp = (node *)n->pointers[0];
			tmp->parent = n;
			neighbor->pointers[neighbor->num_keys] = NULL;
			n->keys[0] = k_prime;
			n->parent->keys[k_prime_index] = neighbor->keys[neighbor->num_keys - 1];
		}
		else {
			n->pointers[0] = neighbor->pointers[neighbor->num_keys - 1];
			neighbor->pointers[neighbor->num_keys - 1] = NULL;
			n->keys[0] = neighbor->keys[neighbor->num_keys - 1];
			n->parent->keys[k_prime_index] = n->keys[0];
		}
	}

	/* Case: n is the leftmost child.
	 * Take a key-pointer pair from the neighbor to the right.
	 * Move the neighbor's leftmost key-pointer pair
	 * to n's rightmost position.
	 */

	else {  
		if (n->is_leaf) {
			n->keys[n->num_keys] = neighbor->keys[0];
			n->pointers[n->num_keys] = neighbor->pointers[0];
			n->parent->keys[k_prime_index] = neighbor->keys[1];
		}
		else {
			n->keys[n->num_keys] = k_prime;
			n->pointers[n->num_keys + 1] = neighbor->pointers[0];
			tmp = (node *)n->pointers[n->num_keys + 1];
			tmp->parent = n;
			n->parent->keys[k_prime_index] = neighbor->keys[0];
		}
		for (i = 0; i < neighbor->num_keys - 1; i++) {
			neighbor->keys[i] = neighbor->keys[i + 1];
			neighbor->pointers[i] = neighbor->pointers[i + 1];
		}
		if (!n->is_leaf)
			neighbor->pointers[i] = neighbor->pointers[i + 1];
	}

	/* n now has one more key and one more pointer;
	 * the neighbor has one fewer of each.
	 */

	n->num_keys++;
	neighbor->num_keys--;

	return root;
}

treeRoot * delete_entryN( treeRoot* root, leafNode * n, int key, int pos) {

	// Remove key and pointer from node.

	n = remove_entry_from_nodeN(n, key, pos);

	/* Case:  deletion from the root. 
	 */

	if (n == root->lNode) 
		return adjust_rootN(root);


	/* Case:  deletion from a node below the root.
	 * (Rest of function body.)
	 */

	/* Determine minimum allowable size of node,
	 * to be preserved after deletion.
	 */

	// min_keys = n->is_leaf ? cut(order - 1) : cut(order) - 1;

	// /* Case:  node stays at or above minimum.
	//  * (The simple case.)
	//  */

	// if (n->num_keys >= min_keys)
	// 	return root;

	// /* Case:  node falls below minimum.
	//  * Either coalescence or redistribution
	//  * is needed.
	//  */

	// /* Find the appropriate neighbor node with which
	//  * to coalesce.
	//  * Also find the key (k_prime) in the parent
	//  * between the pointer to node n and the pointer
	//  * to the neighbor.
	//  */

	// neighbor_index = get_neighbor_index( n );
	// k_prime_index = neighbor_index == -1 ? 0 : neighbor_index;
	// k_prime = n->parent->keys[k_prime_index];
	// neighbor = neighbor_index == -1 ? n->parent->pointers[1] : 
	// 	n->parent->pointers[neighbor_index];

	// capacity = n->is_leaf ? order : order - 1;

	// /* Coalescence. */

	// if (neighbor->num_keys + n->num_keys < capacity)
	// 	return coalesce_nodes(root, n, neighbor, neighbor_index, k_prime);

	// /* Redistribution. */

	// else
	// 	return redistribute_nodes(root, n, neighbor, neighbor_index, k_prime_index, k_prime);

	return root;
}

/* Deletes an entry from the B+ tree.
 * Removes the record and its key and pointer
 * from the leaf, and then makes all appropriate
 * changes to preserve the B+ tree properties.
 */
node * delete_entry( node * root, node * n, int key, void * pointer ) {

	int min_keys;
	node * neighbor;
	int neighbor_index;
	int k_prime_index, k_prime;
	int capacity;

	// Remove key and pointer from node.

	n = remove_entry_from_node(n, key, pointer);

	/* Case:  deletion from the root. 
	 */

	if (n == root) 
		return adjust_root(root);


	/* Case:  deletion from a node below the root.
	 * (Rest of function body.)
	 */

	/* Determine minimum allowable size of node,
	 * to be preserved after deletion.
	 */

	min_keys = n->is_leaf ? cut(order - 1) : cut(order) - 1;

	/* Case:  node stays at or above minimum.
	 * (The simple case.)
	 */

	if (n->num_keys >= min_keys)
		return root;

	/* Case:  node falls below minimum.
	 * Either coalescence or redistribution
	 * is needed.
	 */

	/* Find the appropriate neighbor node with which
	 * to coalesce.
	 * Also find the key (k_prime) in the parent
	 * between the pointer to node n and the pointer
	 * to the neighbor.
	 */

	neighbor_index = get_neighbor_index( n );
	k_prime_index = neighbor_index == -1 ? 0 : neighbor_index;
	k_prime = n->parent->keys[k_prime_index];
	neighbor = neighbor_index == -1 ? n->parent->pointers[1] : 
		n->parent->pointers[neighbor_index];

	capacity = n->is_leaf ? order : order - 1;

	/* Coalescence. */

	if (neighbor->num_keys + n->num_keys < capacity)
		return coalesce_nodes(root, n, neighbor, neighbor_index, k_prime);

	/* Redistribution. */

	else
		return redistribute_nodes(root, n, neighbor, neighbor_index, k_prime_index, k_prime);
}




//deletion.
treeRoot* delete_key(treeRoot* root, int key){
	int base_pos = findN(root, key);
	leafNode* leaf_node = find_leafN(root, key);
	if (leaf_node != NULL) {
		root = delete_entryN(root, leaf_node, key, base_pos);
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
		// free(lNode->keys);
		// free(lNode->pos);
		free(lNode);
		lNode = NULL;
	}else{
		internalNode* iNode = (internalNode*)node;
		for (i = 0; i <= iNode->num_keys; i++)
			destroy_tree_nodes(iNode->pointers[i]);
		// free(iNode->keys);
		// free(iNode->pointers);
		free(iNode);
		iNode = NULL;
	}
}

void destroy_tree(treeRoot * root) {
	if(root->isLeaf){
		// free(root->lNode->keys);
		// free(root->lNode->pos);
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

