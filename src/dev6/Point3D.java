public class Point3D implements Writable<Point3D>
{
	//定义三维空间变量分别为x,y,z
	private float x,y,z;
	//返回x坐标
	public float getX()
		{
		returnx;
		}
	//返回y坐标
	public float getY()
		{
		returny;
	}
	//返回z坐标
	public float getZ()
		{
		returnz;
		}
	//定义readFields（）方法实现读取数据
	public void readFields(DataInputin)throwsIOException
		{
		x=in.readFloat();
		y=in.readFloat();
		z=in.readFloat();
	}
	
	//定义write（）方法实现写入数据
	public void write(DataOutputout)throwsIOException
		{
		out.writeFloat(x);
		out.writeFloat(y);
		out.writeFloat(z);
	}
}

