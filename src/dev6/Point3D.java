public class Point3D implements Writable<Point3D>
{
	//������ά�ռ�����ֱ�Ϊx,y,z
	private float x,y,z;
	//����x����
	public float getX()
		{
		returnx;
		}
	//����y����
	public float getY()
		{
		returny;
	}
	//����z����
	public float getZ()
		{
		returnz;
		}
	//����readFields��������ʵ�ֶ�ȡ����
	public void readFields(DataInputin)throwsIOException
		{
		x=in.readFloat();
		y=in.readFloat();
		z=in.readFloat();
	}
	
	//����write��������ʵ��д������
	public void write(DataOutputout)throwsIOException
		{
		out.writeFloat(x);
		out.writeFloat(y);
		out.writeFloat(z);
	}
}

