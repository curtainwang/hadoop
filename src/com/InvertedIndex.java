package com;
import java.io.IOException;
import java.util.StringTokenizer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class InvertedIndex {
	public static class Map extends Mapper<Object, Text, Text, Text>{
		private Text keyInfo = new Text();
		private Text valueInfo = new Text();
		private FileSplit split;
		
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			split = (FileSplit) context.getInputSplit();
			StringTokenizer itr = new StringTokenizer(value.toString());
		}
		
	}

}
