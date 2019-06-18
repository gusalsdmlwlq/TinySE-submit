package edu.hanyang.submit;

import java.io.IOException;

import edu.hanyang.indexer.DocumentCursor;
import edu.hanyang.indexer.PositionCursor;
import edu.hanyang.indexer.IntermediateList;
import edu.hanyang.indexer.IntermediatePositionalList;
import edu.hanyang.indexer.QueryPlanTree;
import edu.hanyang.indexer.QueryPlanTree.NODE_TYPE;
import edu.hanyang.indexer.QueryPlanTree.QueryPlanNode;
import edu.hanyang.indexer.QueryProcess;
import edu.hanyang.indexer.StatAPI;
import java.util.ArrayList;
import java.util.List;
import java.util.Collections;
import java.util.LinkedList;
import java.util.Queue;

public class TinySEQueryProcess implements QueryProcess {

	@Override
	public void op_and_w_pos(DocumentCursor op1, DocumentCursor op2, int shift, IntermediatePositionalList out)
			throws IOException {
		int doc1, doc2;
		PositionCursor q1, q2;
		int pos1,pos2;
		while(op1.is_eol()==false && op2.is_eol()==false){
			doc1 = op1.get_docid();
			doc2 = op2.get_docid();
			if(doc1 < doc2){
				op1.go_next();
			}
			else if(doc1 > doc2){
				op2.go_next();
			}
			else{
				q1 = op1.get_position_cursor();
				q2 = op2.get_position_cursor();
				while(q1.is_eol()==false && q2.is_eol()==false){
					pos1 = q1.get_pos();
					pos2 = q2.get_pos();
					if(pos1 + shift < pos2){
						q1.go_next();
					}
					else if(pos1 + shift > pos2){
						q2.go_next();
					}
					else{
						out.put_docid_and_pos(doc1, pos1);
						q1.go_next();
						q2.go_next();
					}
				}
				op1.go_next();
				op2.go_next();
			}
		}
	}
	
	@Override
	public void op_and_wo_pos(DocumentCursor op1, DocumentCursor op2, IntermediateList out) throws IOException {
		int doc1, doc2;
		while(op1.is_eol()==false && op2.is_eol()==false){
			doc1 = op1.get_docid();
			doc2 = op2.get_docid();
			if(doc1 < doc2){
				op1.go_next();
			}
			else if(doc1 > doc2){
				op2.go_next();
			}
			else{
				out.put_docid(doc1);
				op1.go_next();
				op2.go_next();
		
			}
		}
	}

	@Override
	public QueryPlanTree parse_query(String query, StatAPI stat) throws Exception {
		QueryPlanTree tree = new QueryPlanTree();
		QueryPlanNode oprand;
		ArrayList<QueryPlanNode> op_list = new ArrayList<QueryPlanNode>();
		QueryPlanNode op;
		int shift = 0;
		Boolean inphase = false;
//		System.out.println(query);
		String[] strs = query.split(" ");
		for(int i=0; i<strs.length; i++){
			if(strs[i].length() == 1 && strs[i].charAt(0) == '"'){ // 예외처리
				if(inphase == false){
					inphase = true;
					shift = 0;
				}
				else{
					inphase = false;
					oprand = tree.new QueryPlanNode();
					oprand.type = NODE_TYPE.OP_REMOVE_POS;
					oprand.left = op_list.get(op_list.size()-1);
					op_list.remove(op_list.size()-1);
//					System.out.println("new op remove pos");
					if(op_list.isEmpty()){
						op_list.add(oprand);
					}
					else{
						op = tree.new QueryPlanNode();
						op.type = NODE_TYPE.OP_AND;
						op.left = op_list.get(op_list.size()-1);
						op_list.remove(op_list.size()-1);
						op.right = oprand;
						op_list.add(op);
//						System.out.println("new op and");
					}
//					System.out.println("phase out "+strs[i]);
				}
				continue;
			}
			
			if(strs[i].charAt(0) == '"'){
				inphase = true;
				shift = 0;
//				System.out.println("phase in "+strs[i]);
			}
			oprand = tree.new QueryPlanNode();
			oprand.type = NODE_TYPE.OPRAND;
			oprand.termid = Integer.parseInt((strs[i].replace('"', ' ')).trim());
//			System.out.println("new oprand "+oprand.termid);
			
			if(inphase == false){
				op = tree.new QueryPlanNode();
				op.type = NODE_TYPE.OP_REMOVE_POS;
				op.left = oprand;
				oprand = op;
//				System.out.println("new op remove pos");
				if(op_list.isEmpty()){
					op_list.add(oprand);
				}
				else{
					op = tree.new QueryPlanNode();
					op.type = NODE_TYPE.OP_AND;
					op.left = op_list.get(op_list.size()-1);
					op_list.remove(op_list.size()-1);
					op.right = oprand;
					op_list.add(op);
//					System.out.println("new op and");
				}
			}
			else{
				if(op_list.isEmpty()==false && (op_list.get(op_list.size()-1).type == NODE_TYPE.OPRAND || op_list.get(op_list.size()-1).type == NODE_TYPE.OP_SHIFTED_AND)){
					op = tree.new QueryPlanNode();
					op.type = NODE_TYPE.OP_SHIFTED_AND;
					op.shift = shift;
					op.left = op_list.get(op_list.size()-1);
					op_list.remove(op_list.size()-1);
					op.right = oprand;
					op_list.add(op);
//					System.out.println("new op shift and");
				}
				else{
					op_list.add(oprand);
				}
				shift++;
			}
			
			
			if(strs[i].charAt(strs[i].length()-1) == '"'){
				inphase = false;
				oprand = tree.new QueryPlanNode();
				oprand.type = NODE_TYPE.OP_REMOVE_POS;
				oprand.left = op_list.get(op_list.size()-1);
				op_list.remove(op_list.size()-1);
//				System.out.println("new op remove pos");
				if(op_list.isEmpty()){
					op_list.add(oprand);
				}
				else{
					op = tree.new QueryPlanNode();
					op.type = NODE_TYPE.OP_AND;
					op.left = op_list.get(op_list.size()-1);
					op_list.remove(op_list.size()-1);
					op.right = oprand;
					op_list.add(op);
//					System.out.println("new op and");
				}
//				System.out.println("phase out "+strs[i]);
			}
		}
		tree.root = op_list.get(0);
		return tree;
	}


}
