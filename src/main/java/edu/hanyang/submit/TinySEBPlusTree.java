package edu.hanyang.submit;

import edu.hanyang.indexer.BPlusTree;

import static org.junit.Assert.assertEquals;

import java.io.RandomAccessFile;
import java.util.ArrayList;
import java.util.Queue;
import java.util.LinkedList;

class Node{
	int is_leaf;
	int n_keys;
	int child_position;
	Node parent;
	ArrayList<Integer> key = new ArrayList<Integer>();
	ArrayList<Integer> value = new ArrayList<Integer>();
	ArrayList<Node> pointer = new ArrayList<Node>();
	public Node(){
		this.n_keys = 0;
		this.is_leaf = 1;
		this.child_position = 0;
		this.parent = null;
	}
}

public class TinySEBPlusTree implements BPlusTree{
	
	int blocksize;
	int nblocks;
	int max_keys;
	Node root;
	RandomAccessFile file;
	String save_path;
	Queue<Integer> child_nums = new LinkedList<Integer>();
	Queue<Node> nodes = new LinkedList<Node>();
	
	public void save(Node root){
//		child 개수, (key, value)
		try{
			if(root.is_leaf == 1){
//				file.writeInt(0);
				file.writeInt(1);
				file.writeInt(root.n_keys);
				for(int i=0; i<root.n_keys; i++){
					file.writeInt(root.key.get(i));
					file.writeInt(root.value.get(i));
				}
//				for(int i=root.n_keys; i<this.max_keys; i++){ // padding(max_key로 모두 맞춤)
//					file.writeInt(0);
//					file.writeInt(0);
//				}
			}
			else{
				file.writeInt(0);
				file.writeInt(root.n_keys+1);
				for(int i=0; i<root.n_keys; i++){
					file.writeInt(root.key.get(i));
					file.writeInt(0);
				}
//				for(int i=root.n_keys; i<this.max_keys; i++){ // padding(max_key로 모두 맞춤)
//					file.writeInt(0);
//					file.writeInt(0);
//				}
				for(int i=0; i<root.n_keys+1; i++){
					this.nodes.offer(root.pointer.get(i));
				}
			}
			
		}
		catch(Exception e){
			System.out.println(e);
		}
	}
	
	@Override
	public void close() {
		try{
			this.file = new RandomAccessFile(this.save_path,"rw");
			this.nodes.add(this.root);
			while(!this.nodes.isEmpty()){
				save(this.nodes.poll());
			}
			this.file.close();
		}
		catch(Exception e){
			System.out.println(e);
		}
	}

	@Override
	public void insert(int key, int value) {
		Node cur = this.root;
		if(cur.is_leaf == 1){ // root가 leaf인 경우 (노드가 한번도 split 되지 않은 경우)
			if(cur.n_keys == 0){ // 노드에 key가 하나도 없는 경우
				cur.key.add(0, key);
				cur.value.add(0, value);
				cur.n_keys++; // key 개수 증가
			}
			if(key < cur.key.get(0)){ // 노드 처음에 추가
				cur.key.add(0, key);
				cur.value.add(0, value);
				cur.n_keys++; // key 개수 증가
				if(cur.n_keys > this.max_keys){
					split(cur);
				}
			}
			else if(key > cur.key.get(cur.n_keys-1)){ // 노드 마지막에 추가
				cur.key.add(cur.n_keys, key);
				cur.value.add(cur.n_keys, value);
				cur.n_keys++; // key 개수 증가
				if(cur.n_keys > this.max_keys){
					split(cur);
				}
			}
			else{ // 노드 사이에 추가
				for(int i=0; i<cur.n_keys-1; i++){
					if(key > cur.key.get(i) && key < cur.key.get(i+1)){
						cur.key.add(i+1, key);
						cur.value.add(i+1, value);
						cur.n_keys++; // key 개수 증가
						break;
					}
				}
				if(cur.n_keys > this.max_keys){
					split(cur);
				}
			}
		}
		else{ // root가 non-leaf
			while(true){
				if(key < cur.key.get(0)){ // 첫 child로
					cur = cur.pointer.get(0);
				}
				else if(key > cur.key.get(cur.n_keys-1)){ // 마지막 child로
					cur = cur.pointer.get(cur.n_keys);
				}
				else{ // 사이의 child로
					for(int i=0; i<cur.n_keys-1; i++){
						if(key > cur.key.get(i) && key < cur.key.get(i+1)){
							cur = cur.pointer.get(i+1);
							break;
						}
					}
				}
				if(cur.is_leaf == 1){
					if(key < cur.key.get(0)){ // 노드 처음에 추가
						cur.key.add(0, key);
						cur.value.add(0, value);
						cur.n_keys++; // key 개수 증가
						if(cur.n_keys > this.max_keys){
							split(cur);
						}
					}
					else if(key > cur.key.get(cur.n_keys-1)){ // 노드 마지막에 추가
						cur.key.add(cur.n_keys, key);
						cur.value.add(cur.n_keys, value);
						cur.n_keys++; // key 개수 증가
						if(cur.n_keys > this.max_keys){
							split(cur);
						}
					}
					else{ // 노드 사이에 추가
						for(int i=0; i<cur.n_keys-1; i++){
							if(key > cur.key.get(i) && key < cur.key.get(i+1)){
								cur.key.add(i+1, key);
								cur.value.add(i+1, value);
								cur.n_keys++; // key 개수 증가
								break;
							}
						}
						if(cur.n_keys > this.max_keys){
							split(cur);
						}
					}
					break;
				}
			}
		}
	}

	@Override
	public void open(String metapath, String savepath, int blocksize, int nblocks) {
		this.blocksize = blocksize;
		this.nblocks = nblocks;
		this.max_keys = blocksize/4;
		int pointer_num, cur_key, cur_value; // 데이터에서 읽는 자식 개수/key/value
		Node new_node; // 데이터를 읽고 정보를 저장할 노드
		int child_num; // 큐에서 가져올 자식 개수
		Node parent_node; // 큐에서 가져올 부모 노드
		this.root = new Node(); //새 루트 정의
		this.save_path = savepath;
		try{
			this.file = new RandomAccessFile(savepath,"r");
			new_node = new Node(); // 새로운 노드 생성
			if(file.readInt() == 0) new_node.is_leaf = 0; // 읽은 노드가 non-leaf
			pointer_num = file.readInt(); // 자식 개수 읽기
			if(new_node.is_leaf == 0) pointer_num--;
			for(int i=0; i<pointer_num; i++){ // 노드 단위로 읽음(max_keys * 2만큼의 int)
				cur_key = file.readInt();
				cur_value = file.readInt();
				if(cur_key > 0 && cur_value > 0){ // key, value 둘다 존재
					new_node.key.add(cur_key);
					new_node.value.add(cur_value);
					new_node.n_keys++;
				}
				else if(cur_key > 0 && cur_value == 0){ // key만 존재(pointer)
					new_node.key.add(cur_key);
					new_node.n_keys++;
//					pointer는 나중에 자식들을 읽는 단계에서 추가
				}
			}
			this.root = new_node;
			if(new_node.is_leaf == 0){
				pointer_num++;
				this.nodes.offer(new_node); // 부모 노드를 저장
				this.child_nums.offer(pointer_num); // 부모 노드의 자식 개수를 저장
			}
			
			while(!this.child_nums.isEmpty()){
				child_num = this.child_nums.poll();
				parent_node = this.nodes.poll();
				for(int i=0; i<child_num; i++){ // 자식 개수만큼 노드 단위로 읽음
					new_node = new Node(); // 새로운 노드 생성
					if(file.readInt() == 0) new_node.is_leaf = 0; // 읽은 노드가 non-leaf
					pointer_num = file.readInt(); // 자식 개수 읽기
					if(new_node.is_leaf == 0) pointer_num--;
					for(int j=0; j<pointer_num; j++){ // 노드 단위로 읽음(max_keys * 2만큼의 int)
						cur_key = file.readInt();
						cur_value = file.readInt();
						if(cur_key > 0 && cur_value > 0){ // key, value 둘다 존재
							new_node.key.add(cur_key);
							new_node.value.add(cur_value);
							new_node.n_keys++;
						}
						else if(cur_key > 0 && cur_value == 0){ // key만 존재(pointer)
							new_node.key.add(cur_key);
							new_node.n_keys++;
//							pointer는 나중에 자식들을 읽는 단계에서 추가
						}
					}
					new_node.child_position = i;
					new_node.parent = parent_node;
					parent_node.pointer.add(new_node);
					if(new_node.is_leaf == 0){
						pointer_num++;
						this.nodes.offer(new_node); // 부모 노드를 저장
						this.child_nums.offer(pointer_num); // 부모 노드의 자식 개수를 저장
					}
				}
			}
		}
		catch(Exception e){
			System.out.println(e);
		}
		
	}
	
	public int search_(int key, Node root){
		if(root.is_leaf == 1){
			for(int i=0; i<root.n_keys; i++){
				if(root.key.get(i) == key){
					return root.value.get(i);
				}
			}
			return 0;
		}
		else{
			for(int i=0; i<root.n_keys; i++){
				if(key < root.key.get(i)){
					return search_(key, root.pointer.get(i));
				}
			}
			return search_(key, root.pointer.get(root.n_keys));
		}
	}
	
	@Override
	public int search(int key) {
		return search_(key, this.root);
	}
	
	public Node split(Node node){
		Node parent;
		Node left = new Node();
		Node right = new Node();
		int parent_index; // parent_index가 새로운 parent가 됨
		
		if(node.parent == null){ // parent가 없는 경우(root 노드인 경우)
			parent = new Node(); // 새로운 parent를 만듬
			parent.is_leaf = 0;
			this.root = parent;
		}
		else parent = node.parent; // 현재 노드의 parent가 존재하는 경우			이거 node 지우면 node.parent도 지워져서 오류 발생하나?
		
		if(node.is_leaf == 1){ // leaf 노드가 split 되는 경우
			if(node.n_keys % 2 == 0) parent_index = node.n_keys/2;
			else parent_index = node.n_keys/2 + 1;
			for(int i=0; i<parent_index; i++){
				left.key.add(node.key.get(i));
				left.n_keys++;
				left.value.add(node.value.get(i));
			}
			left.child_position = node.child_position;
			left.parent = parent;
			for(int i=parent_index; i<node.n_keys; i++){
				right.key.add(node.key.get(i));
				right.n_keys++;
				right.value.add(node.value.get(i));
			}
			right.child_position = node.child_position+1;
			right.parent = parent;
			parent.key.add(node.child_position,node.key.get(parent_index));
			parent.n_keys++;
			try{
				parent.pointer.set(node.child_position, left);
			}
			catch(Exception e){
				parent.pointer.add(node.child_position, left);
			}
			parent.pointer.add(node.child_position+1, right);
			if(left.child_position < parent.n_keys){
				for(int i=right.child_position+1; i<=parent.n_keys; i++){
					parent.pointer.get(i).child_position++;
				}
			}
			if(parent.n_keys > this.max_keys){
				split(parent);
			}
		}
		else{ // non-leaf 노드가 split 되는 경우
			left.is_leaf = 0;
			right.is_leaf = 0;
			parent_index = node.n_keys/2;
			int position_index = 0;
			for(int i=0; i<parent_index; i++){
				left.key.add(node.key.get(i));
				left.n_keys++;
				left.pointer.add(node.pointer.get(i));
				left.pointer.get(position_index).child_position = position_index;
				left.pointer.get(position_index).parent = left;
				position_index++;
			}
			left.pointer.add(node.pointer.get(parent_index));
			left.pointer.get(position_index).child_position = position_index;
			left.pointer.get(position_index).parent = left;
			left.child_position = node.child_position;
			left.parent = parent;
//			int start_position = left.pointer.get(0).child_position;
//			for(int i=0; i<=left.n_keys; i++){
//				left.pointer.get(i).child_position -= start_position;
//			}
			position_index = 0;
			for(int i=parent_index+1; i<node.n_keys; i++){
				right.key.add(node.key.get(i));
				right.n_keys++;
				right.pointer.add(node.pointer.get(i));
				right.pointer.get(position_index).child_position = position_index;
				right.pointer.get(position_index).parent = right;
				position_index++;
			}
			right.pointer.add(node.pointer.get(node.n_keys));
			right.pointer.get(position_index).child_position = position_index;
			right.pointer.get(position_index).parent = right;
			right.child_position = node.child_position+1;
			right.parent = parent;
//			start_position = right.pointer.get(0).child_position;
//			for(int i=0; i<=right.n_keys; i++){
//				right.pointer.get(i).child_position -= start_position;
//			}
			parent.key.add(node.child_position,node.key.get(parent_index));
			parent.n_keys++;
			try{
				parent.pointer.set(node.child_position, left);
			}
			catch(Exception e){
				parent.pointer.add(node.child_position, left);
			}
			parent.pointer.add(node.child_position+1, right);
			if(left.child_position < parent.n_keys){
				for(int i=right.child_position+1; i<=parent.n_keys; i++){
					parent.pointer.get(i).child_position++;
				}
			}
			if(parent.n_keys > this.max_keys){
				split(parent);
			}
		}
		node = null;
		return parent;
	}
	
	public void show(Node root){
		if(root.is_leaf == 1){
			for(int i=0; i<root.n_keys; i++){
				System.out.println(root.key.get(i));
			}
			System.out.println("@"+root.n_keys+" "+root.child_position);
		}
		else{
//			for(int i=0; i<root.n_keys; i++){
//				System.out.println(root.key.get(i));
//			}
//			System.out.println("@"+root.n_keys+" "+root.child_position);
			for(int i=0; i<root.pointer.size(); i++){
				show(root.pointer.get(i));
			}
		}
	}
	
//	public static void main(String[] args){
//		long time_start = System.currentTimeMillis();
//		TinySEBPlusTree tree = new TinySEBPlusTree();
//		tree.open("./tmp/tree", "./tmp/tree", 4096, 500);
//		int i=0;
////		try{
////			RandomAccessFile f = new RandomAccessFile("./tmp/treetest-7500000.data", "r");
////			while(true){
////				tree.insert(f.readInt(), f.readInt());
////				if(i++==1000000){
////					i=0;
////					System.out.println("@@@@");
////				}
////				
////			}
////		}
////		catch(Exception e){
////			System.out.println("@@@@@@@@@@@@@");
////			System.out.println(e);
////		}
//		long time_tree = System.currentTimeMillis();
//		System.out.println(time_tree-time_start);
////		tree.show(tree.root);
////		tree.close();
//		long time_save = System.currentTimeMillis();
//		System.out.println(time_save-time_tree);
////		tree.insert(5, 10);
////		tree.insert(6, 15);
////		tree.insert(4, 20);
////		tree.insert(7, 1);
////		tree.insert(8, 5);
////		tree.insert(17, 7);
////		tree.insert(30, 8);
////		tree.insert(1, 8);
////		tree.insert(58, 1);
////		tree.insert(25, 8);
////		tree.insert(96, 32);
////		tree.insert(21, 8);
////		tree.insert(9, 98);
////		tree.insert(57, 54);
////		tree.insert(157, 54);
////		tree.insert(247, 54);
////		tree.insert(357, 254);
////		tree.insert(557, 54);
////		tree.show(tree.root);
////		assertEquals(tree.search(5), 10);
////		assertEquals(tree.search(6), 15);
////		assertEquals(tree.search(4), 20);
////		assertEquals(tree.search(7), 1);
////		assertEquals(tree.search(8), 5);
////		assertEquals(tree.search(17), 7);
////		assertEquals(tree.search(30), 8);
////		assertEquals(tree.search(1), 8);
////		assertEquals(tree.search(58), 1);
////		assertEquals(tree.search(25), 8);
////		assertEquals(tree.search(96), 32);
////		assertEquals(tree.search(21), 8);
////		assertEquals(tree.search(9), 98);
////		assertEquals(tree.search(57), 54);
////		assertEquals(tree.search(157), 54);
////		assertEquals(tree.search(247), 54);
////		assertEquals(tree.search(357), 254);
////		assertEquals(tree.search(557), 54);
////		tree.close();
////		try{
////			RandomAccessFile f = new RandomAccessFile("./tmp/tree.data", "r");
////			int r;
////			while(true){
////				r = f.readInt();
////				System.out.println(r);
////			}
////		}
////		catch(Exception e){
////			System.out.println(e);
////		}
//	}
}
