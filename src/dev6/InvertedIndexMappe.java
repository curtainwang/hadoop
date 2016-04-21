import  java.io.IOException;
import java.util.StringTokenizer;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class InvertedIndexMapper  extends  Mapper<Text,Text,Text,Text>
{
	@Override
		protectedvoidmap(Textkey,Textvalue,Contextcontext)throwsIOException,InterruptedException
		{
		//设使用了缺省的TextInputFormat，LineRecordReader
		//则主键key:lineoffset;value:linestringTextword=newText();
		//读取所需的FileName
		
		FileSplit fileSplit=(FileSplit)context.getInputSplit();
		String FileName=fileSplit.getPath().getName();
		
		//单词出现位置信息记为FileName@LineOffset
		TextFile Name_LineOffset=newText(FileName+”@”+key.toString());
		StringTokenizer itr=new StringTokenizer(value.toString());
		for(;itr.hasMoreTokens();)
			{
			word.set(itr.nextToken());
			//单词出现位置信息记为FileName@LineOffset
			context.write(word,FileName_LineOffset);
		}
	}
}
