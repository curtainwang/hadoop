public class DBOutput {
	//����StudentRecord��̳��� Writable��DBWritable�ӿ�
	public static class StudentinfoRecord implements Writable,  DBWritable {
		int id;
		String name;
		public StudentinfoRecord()
			{ }
		//������readFields����,ʵ�ֶ�ȡ����
		public void readFields(DataInput in) throws IOException
			{
        this.id = in.readInt();
        this.name = Text.readString(in);
		}
		public void write(DataOutput out) throws IOException
			{
			out.writeInt(this.id);
			Text.writeString(out, this.name);
		}
		
		//����readFields����ʵ�ֶ�������
		public void readFields(ResultSet result) throws SQLException {
			this.id = result.getInt(1);
			this.name = result.getString(2);
		}
		
		//������write������ʵ��д������
		public void write(PreparedStatement stmt) throws SQLException {
			stmt.setInt(1, this.id);
			stmt.setString(2, this.name);
		}
		
		public String toString() {
			return new String(this.id + " " + this.name);
		}
	}
   
   //������MyReducer��ʵ����map��reduce
   public static class MyReducer extends MapReduceBase implements
	   Reducer<LongWritable, Text, StudentinfoRecord, Text> {
	   public void reduce(LongWritable key, Iterator<Text> values,OutputCollector<StudentinfoRecord, Text> output, Reporter  reporter)
          throws IOException 
		   {
		   String[] splits = values.next().toString().split("\t");
		   StudentinfoRecord r = new StudentinfoRecord();
		   r.id = Integer.parseInt(splits[0]);
		   r.name = splits[1];
		   output.collect(r, new Text(r.name));
		 }
	}
	
	//������������main
	public static void main(String[] args) throws IOException {
		JobConf conf = new JobConf(DBOutput.class);
		conf.setInputFormat(TextInputFormat.class);
		conf.setOutputFormat(DBOutputFormat.class);
		FileInputFormat.setInputPaths(conf, new Path("/hua/hua.bcp"));
		
		//����mysql���ݿ�
		DBConfiguration.configureDB(conf, "com.mysql.jdbc.Driver",
          "jdbc:mysql://192.168.3.244:3306/hadoop", "hua", "hadoop");
		
		//��������
		DBOutputFormat.setOutput(conf, "studentinfo", "id", "name");
		conf.setMapperClass(org.apache.hadoop.mapred.lib.IdentityMapper.class);
		conf.setReducerClass(MyReducer.class);
		JobClient.runJob(conf);
	}

}
