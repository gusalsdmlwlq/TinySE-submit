package edu.hanyang.submit;

import edu.hanyang.indexer.BPlusTree;

import static org.junit.Assert.assertEquals;

import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.ArrayList;
import java.util.Queue;

import javax.swing.text.Position;

import java.util.LinkedList;

class Node{
	int is_leaf;
	int n_keys;
	int position;
	int parent_position;
	List<Integer> key = new ArrayList<Integer>();
	List<Integer> value = new ArrayList<Integer>();
	public Node(){
		this.n_keys = 0;
		this.is_leaf = 1;
		this.position = 0;
		this.parent_position = -1;
	}
}

public class TinySEBPlusTree implements BPlusTree{
	
	int blocksize;
	int nblocks;
	int max_keys;
	RandomAccessFile file;
	String save_path;
	Node cur_node;
	int root_position;

	@Override
	public void close(){
		try{
			this.file.close();
		}
		catch(Exception e){
			System.out.println(e);
		}
	}
	
	public Node read_node(int pos){
		Node new_node = new Node();
		byte[] buf = new byte[this.blocksize];
		ByteBuffer buffer;
		int tem_key;
		int tem_val;
		try{
			if(this.file.length() == 4){
				new_node.position = 4;
				return new_node;
			}
			this.file.seek(pos);
			this.file.read(buf);
			buffer = ByteBuffer.wrap(buf);
			new_node.is_leaf = buffer.getInt();
			new_node.parent_position = buffer.getInt();
			new_node.n_keys = buffer.getInt();
			new_node.position = pos;
			for(int i=0; i<new_node.n_keys; i++){
				tem_key = buffer.getInt();
				new_node.key.add(tem_key);
				tem_val = buffer.getInt();
				new_node.value.add(tem_val);
			}
			if(new_node.is_leaf == 0){ // non-leaf는 key보다 value가 하나 많으므로 한번 더 읽음
				tem_val = buffer.getInt();
				new_node.value.add(tem_val);
			}
		}
		catch(Exception e){
			System.out.println("read node "+e);
		}
		return new_node;
	}
	
	public void write_node(Node node){
		byte[] buf = new byte[this.blocksize];
		ByteBuffer buffer = ByteBuffer.wrap(buf);
		List<Integer> keys = node.key;
		List<Integer> vals = node.value;
		int n_keys = node.n_keys;
		try{
			buffer.putInt(node.is_leaf);
			buffer.putInt(node.parent_position);
			buffer.putInt(n_keys);
			for(int i=0; i<n_keys; i++){
				buffer.putInt(keys.get(i));
				buffer.putInt(vals.get(i));
			}
			if(node.is_leaf == 0){
				buffer.putInt(vals.get(n_keys));
			}
//			for(int i=ints; i<this.blocksize/4; i++){
//				buffer.putInt(0);
//			}
			this.file.seek(node.position);
			this.file.write(buf);
		}
		catch(Exception e){
			System.out.println("write node "+e);
		}
		keys = null;
		vals = null;
	}
	
	@Override
	public void insert(int key, int value) {
		Node cur = read_node(this.root_position);
		List<Integer> keys = cur.key;
		List<Integer> vals = cur.value;
		if(cur.is_leaf == 1){ // root가 leaf인 경우 (노드가 한번도 split 되지 않은 경우)
			if(cur.n_keys == 0){ // 노드에 key가 하나도 없는 경우
				cur.key.add(0, key);
				cur.value.add(0, value);
				cur.n_keys++; // key 개수 증가
				write_node(cur);
			}
			else if(key < keys.get(0)){ // 노드 처음에 추가
				cur.key.add(0, key);
				cur.value.add(0, value);
				cur.n_keys++; // key 개수 증가
				if(cur.n_keys > this.max_keys){
					split(cur);
				}
				else{
					write_node(cur);
				}
			}
			else if(key > keys.get(cur.n_keys-1)){ // 노드 마지막에 추가
				cur.key.add(cur.n_keys, key);
				cur.value.add(cur.n_keys, value);
				cur.n_keys++; // key 개수 증가
				if(cur.n_keys > this.max_keys){
					split(cur);
				}
				else{
					write_node(cur);
				}
			}
			else{ // 노드 사이에 추가
				for(int i=0; i<cur.n_keys-1; i++){
					if(key > keys.get(i) && key < keys.get(i+1)){
						cur.key.add(i+1, key);
						cur.value.add(i+1, value);
						cur.n_keys++; // key 개수 증가
						break;
					}
				}
				if(cur.n_keys > this.max_keys){
					split(cur);
				}
				else{
					write_node(cur);
				}
			}
		}
		else{ // root가 non-leaf
			while(true){
				if(key < keys.get(0)){ // 첫 child로
					cur = read_node(vals.get(0));
				}
				else if(key > keys.get(cur.n_keys-1)){ // 마지막 child로
					cur = read_node(vals.get(cur.n_keys));
				}
				else{ // 사이의 child로
					for(int i=0; i<cur.n_keys-1; i++){
						if(key > keys.get(i) && key < keys.get(i+1)){
							cur = read_node(vals.get(i+1));
							break;
						}
					}
				}
				keys = cur.key;
				vals = cur.value;
				if(cur.is_leaf == 1){
					if(key < keys.get(0)){ // 노드 처음에 추가
						cur.key.add(0, key);
						cur.value.add(0, value);
						cur.n_keys++; // key 개수 증가
						if(cur.n_keys > this.max_keys){
							split(cur);
						}
						else{
							write_node(cur);
						}
					}
					else if(key > keys.get(cur.n_keys-1)){ // 노드 마지막에 추가
						cur.key.add(cur.n_keys, key);
						cur.value.add(cur.n_keys, value);
						cur.n_keys++; // key 개수 증가
						if(cur.n_keys > this.max_keys){
							split(cur);
						}
						else{
							write_node(cur);
						}
					}
					else{ // 노드 사이에 추가
						for(int i=0; i<cur.n_keys-1; i++){
							if(key > keys.get(i) && key < keys.get(i+1)){
								cur.key.add(i+1, key);
								cur.value.add(i+1, value);
								cur.n_keys++; // key 개수 증가
								break;
							}
						}
						if(cur.n_keys > this.max_keys){
							split(cur);
						}
						else{
							write_node(cur);
						}
					}
					break;
				}
			}
		}
		keys = null;
		vals = null;
	}

	@Override
	public void open(String metapath, String savepath, int blocksize, int nblocks) {
		this.blocksize = blocksize; // blocksize 단위로 저장
		this.nblocks = nblocks;
		this.max_keys = blocksize/8 - 2;
		this.save_path = savepath;
		try{
			this.file = new RandomAccessFile(savepath,"rw");
			if(file.length() == 0){
				this.root_position = 4;
				this.file.writeInt(this.root_position);
			}
			else{
				this.root_position = this.file.readInt();
			}
		}
		catch(Exception e){
			System.out.println("open "+e);
		}
		
	}
	
	public int search_(int key, int root){
		Node cur = read_node(root);
		List<Integer> keys = cur.key;
		List<Integer> vals = cur.value;
		if(cur.is_leaf == 1){
			for(int i=0; i<cur.n_keys; i++){
				if(keys.get(i) == key){
					return vals.get(i);
				}
			}
			return 0;
		}
		else{
			for(int i=0; i<cur.n_keys; i++){
				if(key < keys.get(i)){
					return search_(key, vals.get(i));
				}
			}
			return search_(key, vals.get(cur.n_keys));
		}
	}
	
	@Override
	public int search(int key) {
		return search_(key, this.root_position);
	}
	
	public Node split(Node node){
		Node parent;
		Node left = new Node();
		Node right = new Node();
		int parent_index = node.parent_position; // parent_index가 새로운 parent가 됨
		int is_new_root = 0;
		List<Integer> keys = node.key;
		List<Integer> vals = node.value;
		
		if(node.parent_position == -1){ // parent가 없는 경우(root 노드인 경우)
			parent = new Node(); // 새로운 parent를 만듬
			parent.is_leaf = 0;
			is_new_root = 1;
		}
		else parent = read_node(parent_index); // 현재 노드의 parent가 존재하는 경우			이거 node 지우면 node.parent도 지워져서 오류 발생하나?
		
		try{
			if(is_new_root == 1){
				parent.position = (int)this.file.length()+this.blocksize;
				this.root_position = parent.position;
			}
			if(node.is_leaf == 1){ // leaf 노드가 split 되는 경우
				if(node.n_keys % 2 == 0) parent_index = node.n_keys/2;
				else parent_index = node.n_keys/2 + 1;
				for(int i=0; i<parent_index; i++){
					left.key.add(keys.get(i));
					left.n_keys++;
					left.value.add(vals.get(i));
				}
				left.position = node.position;
				left.parent_position = parent.position;
				write_node(left);
				for(int i=parent_index; i<node.n_keys; i++){
					right.key.add(keys.get(i));
					right.n_keys++;
					right.value.add(vals.get(i));
				}
				right.position = (int)this.file.length();
				right.parent_position = parent.position;
				write_node(right);
				if(parent.n_keys == 0 || parent.key.get(0) > keys.get(parent_index)){
					parent.key.add(0,keys.get(parent_index));
					parent.n_keys++;
					if(parent.n_keys == 1) parent.value.add(0, left.position);
					else parent.value.set(0, left.position);
					parent.value.add(1, right.position);
				}
				else if(parent.key.get(parent.n_keys-1) < keys.get(parent_index))
				{
					parent.key.add(keys.get(parent_index));
					parent.n_keys++;
					parent.value.set(parent.n_keys-1, left.position);
					parent.value.add(right.position);
				}
				else{
					for(int i=1; i<parent.n_keys; i++){
						if(parent.key.get(i) > keys.get(parent_index)){
							parent.key.add(i,keys.get(parent_index));
							parent.n_keys++;
							parent.value.set(i, left.position);
							parent.value.add(i+1, right.position);
							break;
						}
					}
				}
				if(parent.n_keys > this.max_keys){
					split(parent);
				}
				else{
					if(is_new_root == 1){
						this.file.seek(0);
						this.file.writeInt(parent.position);
					}
					write_node(parent);
				}
			}
			else{ // non-leaf 노드가 split 되는 경우
				left.is_leaf = 0;
				right.is_leaf = 0;
				parent_index = node.n_keys/2;
				for(int i=0; i<parent_index; i++){
					left.key.add(keys.get(i));
					left.n_keys++;
					left.value.add(vals.get(i));
				}
				left.value.add(vals.get(parent_index));
				left.position = node.position;
				left.parent_position = parent.position;
				write_node(left);
				for(int i=parent_index+1; i<node.n_keys; i++){
					right.key.add(keys.get(i));
					right.n_keys++;
					right.value.add(vals.get(i));
				}
				right.value.add(vals.get(node.n_keys));
				right.position = (int)this.file.length();
				right.parent_position= parent.position;
				write_node(right);
				
				for(int i=0; i<=right.n_keys; i++){
					Node temp = read_node(right.value.get(i));
					temp.parent_position = right.position;
					write_node(temp);
				}
				
				
				
				if(parent.n_keys == 0 || parent.key.get(0) > keys.get(parent_index)){
					parent.key.add(0,keys.get(parent_index));
					parent.n_keys++;
					if(parent.n_keys == 1) parent.value.add(0, left.position);
					else parent.value.set(0, left.position);
					parent.value.add(1, right.position);
				}
				else if(parent.key.get(parent.n_keys-1) < keys.get(parent_index))
				{
					parent.key.add(keys.get(parent_index));
					parent.n_keys++;
					parent.value.set(parent.n_keys-1, left.position);
					parent.value.add(right.position);
				}
				else{
					for(int i=1; i<parent.n_keys; i++){
						if(parent.key.get(i) > keys.get(parent_index)){
							parent.key.add(i,keys.get(parent_index));
							parent.n_keys++;
							parent.value.set(i, left.position);
							parent.value.add(i+1, right.position);
							break;
						}
					}
				}
				if(parent.n_keys > this.max_keys){
					split(parent);
				}
				else{
					if(is_new_root == 1){
						this.file.seek(0);
						this.file.writeInt(parent.position);
					}
					write_node(parent);
				}
			}
		}
		catch(Exception e){
			System.out.println("split "+e);
		}
		keys = null;
		vals = null;
		return parent;
	}
	
	public void show(int root){
		Node cur = read_node(root);
		if(cur.is_leaf == 1){
			for(int i=0; i<cur.n_keys; i++){
				System.out.println(cur.key.get(i));
			}
		}
		else{
			for(int i=0; i<cur.value.size(); i++){
				show(cur.value.get(i));
			}
		}
	}
}
