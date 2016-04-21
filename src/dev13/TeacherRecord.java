public class TeacherRecord implements Writable, DBWritable{
 
      int id;
      String name;
      int age;
      int departmentID;
     
      @Override
		  //定义readFields()方法
      public void readFields(DataInput in) throws IOException {
		  // TODO Auto-generated method stub
		  //readInt()方法读取整形
		  
		  this.id = in.readInt();
		  //Text.readString()读取文本类型
		  this.name = Text.readString(in);
		  this.age = in.readInt();
		  this.departmentID = in.readInt();
		 }
		
		 //定义一个write()方法
		 public void write(DataOutput out) throws IOException {
			 // TODO Auto-generated method stub
			 //writeInt()写入整形
			 out.writeInt(this.id);
			 
			 //writeString()写入字符串
             Text.writeString(out, this.name);
             out.writeInt(this.age);
             out.writeInt(this.departmentID);
			}
			
		//定义一个readFields()方法
		public void readFields(ResultSet result) throws SQLException {
			// TODO Auto-generated method stub
			this.id = result.getInt(1);
			this.name = result.getString(2);
			this.age = result.getInt(3);
			this.departmentID = result.getInt(4);
		}
		
		//重写 write方法，功能实现写入操作
		public void write(PreparedStatement stmt) throws SQLException {
			// TODO Auto-generated method stub
			stmt.setInt(1, this.id);
			stmt.setString(2, this.name);
			stmt.setInt(3, this.age);
			stmt.setInt(4, this.departmentID);
		}
		
		public String toString() {
			// TODO Auto-generated method stub
			return new String(this.name + " " + this.age + " " + this.departmentID);
		}
}
