package edu.hanyang.submit;

import java.io.IOException;

import edu.hanyang.indexer.ExternalSort;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import org.apache.commons.lang3.tuple.Triple;
import java.io.File;

public class TinySEExternalSort implements ExternalSort {
	
	class TripleSort implements Comparator<Triple<Integer,Integer,Integer>> {
		@Override 
		public int compare(Triple<Integer,Integer,Integer> a, Triple<Integer,Integer,Integer> b) { 
			if(a.getLeft() > b.getLeft()) return 1;
			else if(a.getLeft() < b.getLeft()) return -1;
			else{
				if(a.getMiddle() > b.getMiddle()) return 1;
				else if(a.getMiddle() < b.getMiddle()) return -1;
				else{
					if(a.getRight() > b.getRight()) return 1;
					else return -1;
				}
			} 
		} 
	}
	public int compTriple(Triple<Integer,Integer,Integer> a, Triple<Integer,Integer,Integer> b) { 
		if(a.getLeft() > b.getLeft()) return 1;
		else if(a.getLeft() < b.getLeft()) return -1;
		else{
			if(a.getMiddle() > b.getMiddle()) return 1;
			else if(a.getMiddle() < b.getMiddle()) return -1;
			else{
				if(a.getRight() > b.getRight()) return 1;
				else return -1;
			}
		} 
	}
	public void sort(String infile, String outfile, String tmpdir, int blocksize, int nblocks) throws IOException {
		File dir = new File(tmpdir);
		if(!dir.exists()){
			dir.mkdirs();
		}
//		int bufsize = records * ((Integer.SIZE/Byte.SIZE) * 3);
		DataInputStream input = new DataInputStream(new BufferedInputStream(new FileInputStream(infile),blocksize));
		DataOutputStream output = new DataOutputStream(new BufferedOutputStream(new FileOutputStream(outfile),blocksize));
		DataOutputStream run_writer;
		DataInputStream run_read1;
		DataInputStream run_read2;
		ArrayList<Triple<Integer, Integer, Integer>> runs = new ArrayList<Triple<Integer, Integer, Integer>>();
		int records = blocksize / ((Integer.SIZE/Byte.SIZE) * 3); // 한 블럭당 튜플 개수
		int word_id, doc_id, pos;
		int run = records * 3 * (Integer.SIZE/Byte.SIZE) * nblocks; // 한 run의 용량
		int run_cnt = 1;
		int path_cnt = 1;
		while(input.available() >= run){
			for(int i=0; i<records * nblocks; i++){
				word_id = input.readInt();
				doc_id = input.readInt();
				pos = input.readInt();
				runs.add(Triple.of(word_id,doc_id,pos));
			}
			Collections.sort(runs, new TripleSort());
			run_writer = new DataOutputStream(new BufferedOutputStream(new FileOutputStream(tmpdir+"/run_"+path_cnt+"_"+run_cnt+".data"),blocksize));
			for(Triple<Integer,Integer,Integer> tuple : runs){
				run_writer.writeInt(tuple.getLeft());
				run_writer.writeInt(tuple.getMiddle());
				run_writer.writeInt(tuple.getRight());
			}
			run_writer.close();
			System.out.println((run_cnt++)+" runs");
			runs.clear();
		}
		int remains = input.available() / ((Integer.SIZE/Byte.SIZE) * 3);
		for(int i=0; i<remains; i++){
			word_id = input.readInt();
			doc_id = input.readInt();
			pos = input.readInt();
			runs.add(Triple.of(word_id,doc_id,pos));
		}
		Collections.sort(runs, new TripleSort());
		run_writer = new DataOutputStream(new BufferedOutputStream(new FileOutputStream(tmpdir+"/run_"+path_cnt+"_"+run_cnt+".data"),blocksize));
		for(Triple<Integer,Integer,Integer> tuple : runs){
			run_writer.writeInt(tuple.getLeft());
			run_writer.writeInt(tuple.getMiddle());
			run_writer.writeInt(tuple.getRight());
		}
		run_writer.close();
		System.out.println((run_cnt)+" runs");
		System.out.println("create runs");
		input.close();
		// create run 완료
		// merge path 시작
		int word_id2, doc_id2, pos2;
		Triple<Integer,Integer,Integer> triple1;
		Triple<Integer,Integer,Integer> triple2;
		while(true){
			int pre_runs = run_cnt;
			path_cnt++;
			run_cnt = 1;
			for(int i=1; i<=pre_runs/2; i++){
				run_read1 = new DataInputStream(new BufferedInputStream(new FileInputStream(tmpdir+"/run_"+(path_cnt-1)+"_"+(run_cnt*2-1)+".data"),blocksize));
				run_read2 = new DataInputStream(new BufferedInputStream(new FileInputStream(tmpdir+"/run_"+(path_cnt-1)+"_"+(run_cnt*2)+".data"),blocksize));
				run_writer = new DataOutputStream(new BufferedOutputStream(new FileOutputStream(tmpdir+"/run_"+path_cnt+"_"+run_cnt+".data"),blocksize));
				word_id = run_read1.readInt();
				doc_id = run_read1.readInt();
				pos = run_read1.readInt();
				triple1 = Triple.of(word_id,doc_id,pos);
				word_id2 = run_read2.readInt();
				doc_id2 = run_read2.readInt();
				pos2 = run_read2.readInt();
				triple2 = Triple.of(word_id2,doc_id2,pos2);
				while(true){
					if(compTriple(triple1,triple2) == 1){
						run_writer.writeInt(triple2.getLeft());
						run_writer.writeInt(triple2.getMiddle());
						run_writer.writeInt(triple2.getRight());
						if(run_read2.available() <= 0){
							run_writer.writeInt(triple1.getLeft());
							run_writer.writeInt(triple1.getMiddle());
							run_writer.writeInt(triple1.getRight());
							while(run_read1.available() > 0){
								run_writer.writeInt(run_read1.readInt());
							}
							break;
						}
						else{
							word_id2 = run_read2.readInt();
							doc_id2 = run_read2.readInt();
							pos2 = run_read2.readInt();
							triple2 = Triple.of(word_id2,doc_id2,pos2);
						}
					}
					else if(compTriple(triple1,triple2) == -1){
						run_writer.writeInt(triple1.getLeft());
						run_writer.writeInt(triple1.getMiddle());
						run_writer.writeInt(triple1.getRight());
						if(run_read1.available() <= 0){
							run_writer.writeInt(triple2.getLeft());
							run_writer.writeInt(triple2.getMiddle());
							run_writer.writeInt(triple2.getRight());
							while(run_read2.available() > 0){
								run_writer.writeInt(run_read2.readInt());
							}
							break;
						}
						else{
							word_id = run_read1.readInt();
							doc_id = run_read1.readInt();
							pos = run_read1.readInt();
							triple1 = Triple.of(word_id,doc_id,pos);
						}
					}
				}
				System.out.println((run_cnt++)+" runs");
				run_writer.close();
			}
			if(pre_runs%2 == 1){ // run이 홀수인경우 다음 마지막 run을 path로 그대로 넘김
				run_read1 = new DataInputStream(new BufferedInputStream(new FileInputStream(tmpdir+"/run_"+(path_cnt-1)+"_"+pre_runs+".data"),blocksize));
				run_writer = new DataOutputStream(new BufferedOutputStream(new FileOutputStream(tmpdir+"/run_"+path_cnt+"_"+(pre_runs/2+1)+".data"),blocksize));
				while(run_read1.available() > 0){
					run_writer.writeInt(run_read1.readInt());
				}
				run_writer.close();
				System.out.println((run_cnt++)+" runs(copied)");
			}
			if(run_cnt == 3){
				break;
			}
			else{
				System.out.println((path_cnt-1)+" merge");
				run_cnt--;
			}
		}
//		merge path 완료
//		마지막 merge
		run_read1 = new DataInputStream(new BufferedInputStream(new FileInputStream(tmpdir+"/run_"+(path_cnt)+"_1.data"),blocksize));
		run_read2 = new DataInputStream(new BufferedInputStream(new FileInputStream(tmpdir+"/run_"+(path_cnt)+"_2.data"),blocksize));
		word_id = run_read1.readInt();
		doc_id = run_read1.readInt();
		pos = run_read1.readInt();
		triple1 = Triple.of(word_id,doc_id,pos);
		word_id2 = run_read2.readInt();
		doc_id2 = run_read2.readInt();
		pos2 = run_read2.readInt();
		triple2 = Triple.of(word_id2,doc_id2,pos2);
		while(true){
			if(compTriple(triple1,triple2) == 1){
				output.writeInt(triple2.getLeft());
				output.writeInt(triple2.getMiddle());
				output.writeInt(triple2.getRight());
				if(run_read2.available() <= 0){
					output.writeInt(triple1.getLeft());
					output.writeInt(triple1.getMiddle());
					output.writeInt(triple1.getRight());
					while(run_read1.available() > 0){
						output.writeInt(run_read1.readInt());
					}
					break;
				}
				else{
					word_id2 = run_read2.readInt();
					doc_id2 = run_read2.readInt();
					pos2 = run_read2.readInt();
					triple2 = Triple.of(word_id2,doc_id2,pos2);
				}
			}
			else if(compTriple(triple1,triple2) == -1){
				output.writeInt(triple1.getLeft());
				output.writeInt(triple1.getMiddle());
				output.writeInt(triple1.getRight());
				if(run_read1.available() <= 0){
					output.writeInt(triple2.getLeft());
					output.writeInt(triple2.getMiddle());
					output.writeInt(triple2.getRight());
					while(run_read2.available() > 0){
						output.writeInt(run_read2.readInt());
					}
					break;
				}
				else{
					word_id = run_read1.readInt();
					doc_id = run_read1.readInt();
					pos = run_read1.readInt();
					triple1 = Triple.of(word_id,doc_id,pos);
				}
			}
		}
		output.close();
		System.out.println("Finished");
	}
//	public static void main(String[] args){
//		try{
//			long start = System.currentTimeMillis();
//			TinySEExternalSort ts = new TinySEExternalSort();
//			ts.sort(
//					"C:/Users/jeon/jhm/TinySE-submit/src/test/resources/test.data",
//					"C:/Users/jeon/jhm/TinySE-submit/src/test/resources/output.data",
//					"C:/Users/jeon/jhm/TinySE-submit/src/test/resources/tmp/",
//					1024,
//					160);
//			System.out.println(System.currentTimeMillis()-start+" msecs");
//		}
//		catch(Exception e){
//			System.out.println(e);
//		}
//	}
}