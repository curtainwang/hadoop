import  java.io.IOException;
import java.util.StringTokenizer;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class InvertedIndexMapper  extends  Mapper<Text,Text,Text,Text>
{
	@Override
		protectedvoidmap(Textkey,Textvalue,Contextcontext)throwsIOException,InterruptedException
		{
		//��ʹ����ȱʡ��TextInputFormat��LineRecordReader
		//������key:lineoffset;value:linestringTextword=newText();
		//��ȡ�����FileName
		
		FileSplit fileSplit=(FileSplit)context.getInputSplit();
		String FileName=fileSplit.getPath().getName();
		
		//���ʳ���λ����Ϣ��ΪFileName@LineOffset
		TextFile Name_LineOffset=newText(FileName+��@��+key.toString());
		StringTokenizer itr=new StringTokenizer(value.toString());
		for(;itr.hasMoreTokens();)
			{
			word.set(itr.nextToken());
			//���ʳ���λ����Ϣ��ΪFileName@LineOffset
			context.write(word,FileName_LineOffset);
		}
	}
}
