/*
 * 实现WritableComparable接口
 * 定制的Writable类TextPair，存储一对字符串
 */

package com.hadoop.writable;

import java.io.*;

import org.apache.hadoop.*;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

public class TextPair implements WritableComparable<TextPair>{

	/**
	 * @param args
	 */
//	public static void main(String[] args) {
//		// TODO Auto-generated method stub
//
//	}
	
	private Text first;
	private Text second;
	
	public TextPair(){
		set(new Text(), new Text());
	}

	public TextPair(String first, String second){
		set(new Text(first), new Text(second));
	}
	
	public TextPair(Text first, Text second){
		set(first, second);
	}
	
	public void set(Text first, Text second){
		this.first = first;
		this.second = second;
	}
	
	public Text getFirst(){
		return first;
	}
	
	public Text getSecond(){
		return second;
	}
	
	@Override
	public void readFields(DataInput in) throws IOException {
		// TODO Auto-generated method stub
		first.readFields(in);
		second.readFields(in);
	}

	@Override
	public void write(DataOutput out) throws IOException {
		// TODO Auto-generated method stub
		first.write(out);
		second.write(out);
	}
	
	@Override
	public int hashCode(){
		return first.hashCode()*163 + second.hashCode();
	}
	
	@Override
	public boolean equals(Object o){
		if(o instanceof TextPair){
			TextPair tp = (TextPair) o;
			return first.equals(tp.first) && second.equals(tp.second);
		}
		return false;
	}
	
	@Override
	public String toString(){
		return first + "\t"+ second;
	}

	/*
	 * (non-Javadoc)
	 * @see java.lang.Comparable#compareTo(java.lang.Object)排序依据
	 */
	@Override
	public int compareTo(TextPair tp) {
		// TODO Auto-generated method stub
		int cmp = first.compareTo(tp.first);
		
		if(cmp!=0){
			return cmp;
		}
		
		return second.compareTo(tp.second);
		
//		return 0;
	}

}
