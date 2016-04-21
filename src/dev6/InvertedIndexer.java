public class  InvertedIndexer
{
	//主函数 main
	public static void main(String[]args)
		{
		try{
			//创建Configuration类实例conf
			Configuration conf=new Configuration();
			//创建job作业
			job=new Job(conf,"invertindex");job.setJarByClass(InvertedIndexer.class);
			job.setInputFormatClass(FileNameLocInputFormat.class);
			job.set MapperClass(InvertedIndexMapper.class);
			job.setReducerClass(InvertedIndexReducer.class);
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(Text.class);
			FileInputFormat.addInputPath(job,newPath(args[0]));FileOutputFormat.setOutputPath(job,newPath(args[1]));
			System.exit(job.waitForCompletion(true)?0:1);
		}
		catch(Exceptione){e.printStackTrace();{}
	}
}

//通过定制和使用新的数据输入格式和RecordReader，将原来的简单倒排索引程序改造为：
import java.io.IOException;
importjava.util.StringTokenizer;
importorg.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

//创建InvertedIndexMapper类继承了Mapper
public class InvertedIndexMapper extends Mapper<Text,Text,Text,Text>
{
	@Override
		//创建map函数
	protected void map(Textkey,Textvalue,Contextcontext)throwsIOException,InterruptedException
		{
		//InputFormat:FileNameLocInputFormat
		//RecordReader:FileNameLocRecordReader
		//key:filename@lineoffset;value:linestring
		
		Textword=newText();
		StringTokenizer itr=new StringTokenizer(value.toString());
		for(;itr.hasMoreTokens()
			{
			word.set(itr.nextToken());
			context.write(word,key);
		}
	}
}
