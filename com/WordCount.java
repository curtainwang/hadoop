package com;

import java.io.IOException;
import java.util.StringTokenizer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class WordCount {
	/*
	 �����ʵ��Mapper����е�map����
	 ��������е�value���ı��ļ��е�һ��
	 ����StringTokenizer������ַ�����ɵ��ʣ�
	 Ȼ��������<���ʣ�1>
	 д�뵽org.apache.hadoop.mapred.OutputCollector�� 
	*/
	public static class WordCountMap extends Mapper<LongWritable, Text, Text, IntWritable> {
		
		private final IntWritable one = new IntWritable(1);
		private Text word = new Text();
		/*
		 �����е�LongWritable��IntWritable��Text����Hadoop��ʵ�ֵ����ڷ�װjava�������͵��࣬
		 ��Щ�඼�ܹ������л��Ӷ������ڷֲ�ʽ�����н������ݽ�����
		 ���Խ����Ƿֱ���Ϊlong��in��String�����Ʒ
		*/

		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String line = value.toString();
			StringTokenizer token = new StringTokenizer(line);
			while (token.hasMoreTokens()) {
				word.set(token.nextToken());
				context.write(word, one); //output.collect(word, one);   OutputCollector<Text, IntWritable output, Reporter reporter
			}
		}
	}

	/*
	 �����ʵ��Reducer�ӿ��е�reduce��������������е�key��values����Map����������м�����values��һ��Iterator
	*/
	public static class WordCountReduce extends Reducer<Text, IntWritable, Text, IntWritable> {
		public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
			int sum = 0;
			//�������Iterator���Ϳ��Եõ�����ͬһ��key������values���˴���key��һ�����ʣ�values�Ǵ�Ƶ
			for (IntWritable val : values) {
				sum += val.get();
			}
			context.write(key, new IntWritable(sum));
		}
	}

	public static void main(String[] args) throws Exception {
		/*
		 ��hadoop��һ�μ��������Ϊһ��job������ͨ��һ��JobConf������������������job
		 �˴������������key��������Text��value��������IntWritable
		*/
		
		Configuration conf = new Configuration();
		Job job = new Job(conf);
		job.setJarByClass(WordCount.class);
		job.setJobName("wordcount");
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		job.setMapperClass(WordCountMap.class);
		job.setReducerClass(WordCountReduce.class);
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		job.waitForCompletion(true);
		
		/*
		 ToolRunner��run������ʼ��run������Ҫ3����������һ����һ��Configuration���ʵ�����ڶ�����WordCount���ʵ����args���Ǵӿ���̨���յ�������������
		*/
		
	}
}
