public class DBOutput {
	//定义StudentRecord类继承了 Writable、DBWritable接口
	public static class StudentinfoRecord implements Writable,  DBWritable {
		int id;
		String name;
		public StudentinfoRecord()
			{ }
		//定义了readFields方法,实现读取数据
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
		
		//定义readFields方法实现读入数据
		public void readFields(ResultSet result) throws SQLException {
			this.id = result.getInt(1);
			this.name = result.getString(2);
		}
		
		//定义了write方法，实现写入数据
		public void write(PreparedStatement stmt) throws SQLException {
			stmt.setInt(1, this.id);
			stmt.setString(2, this.name);
		}
		
		public String toString() {
			return new String(this.id + " " + this.name);
		}
	}
   
   //定义了MyReducer类实现了map和reduce
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
	
	//定义了主函数main
	public static void main(String[] args) throws IOException {
		JobConf conf = new JobConf(DBOutput.class);
		conf.setInputFormat(TextInputFormat.class);
		conf.setOutputFormat(DBOutputFormat.class);
		FileInputFormat.setInputPaths(conf, new Path("/hua/hua.bcp"));
		
		//连接mysql数据库
		DBConfiguration.configureDB(conf, "com.mysql.jdbc.Driver",
          "jdbc:mysql://192.168.3.244:3306/hadoop", "hua", "hadoop");
		
		//插入数据
		DBOutputFormat.setOutput(conf, "studentinfo", "id", "name");
		conf.setMapperClass(org.apache.hadoop.mapred.lib.IdentityMapper.class);
		conf.setReducerClass(MyReducer.class);
		JobClient.runJob(conf);
	}

}
