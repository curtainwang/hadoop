public class  InvertedIndexer
{
	//������ main
	public static void main(String[]args)
		{
		try{
			//����Configuration��ʵ��conf
			Configuration conf=new Configuration();
			//����job��ҵ
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

//ͨ�����ƺ�ʹ���µ����������ʽ��RecordReader����ԭ���ļ򵥵��������������Ϊ��
import java.io.IOException;
importjava.util.StringTokenizer;
importorg.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

//����InvertedIndexMapper��̳���Mapper
public class InvertedIndexMapper extends Mapper<Text,Text,Text,Text>
{
	@Override
		//����map����
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
