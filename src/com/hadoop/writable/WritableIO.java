
/*
 * 序列化和反序列化
 */

package com.hadoop.writable;

import java.io.*;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Writable;


public class WritableIO {

	/**
	 * @param args
	 */
	public static void main(String[] args) throws IOException{
		// TODO Auto-generated method stub
		IntWritable writable = new IntWritable();
		writable.set(123321);
		//将writable序列化为byte数组
		byte[]  bytes = serialize(writable);
		for(int i = 0; i < bytes.length; i++){
			System.out.print(bytes[i]);
		}
			
		System.out.println("");
		
		IntWritable writable2 = new IntWritable();
		//byte[] -> IntWritable
		deserialize(writable2, bytes);
		System.out.println(writable2.get());

	}
	
	
	//序列化：将writable对象序列化为byte数组
	public static byte[] serialize(Writable writable) throws IOException{
		ByteArrayOutputStream out = new ByteArrayOutputStream();
		DataOutputStream dataOut = new DataOutputStream(out);
		writable.write(dataOut);
		dataOut.close();
		return  out.toByteArray();
	}
	
	
	//反序列化：byte[] -> writable
	public static byte[] deserialize(Writable writable, byte[] bytes) throws IOException{
		ByteArrayInputStream in = new ByteArrayInputStream(bytes);
		DataInputStream dataIn = new DataInputStream(in);
		writable.readFields(dataIn);
		dataIn.close();
		return bytes;
	}
	

}
