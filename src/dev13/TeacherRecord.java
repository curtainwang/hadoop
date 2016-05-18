public class TeacherRecord implements Writable, DBWritable{
 
      int id;
      String name;
      int age;
      int departmentID;
     
      @Override
		  //����readFields()����
      public void readFields(DataInput in) throws IOException {
		  // TODO Auto-generated method stub
		  //readInt()������ȡ����
		  
		  this.id = in.readInt();
		  //Text.readString()��ȡ�ı�����
		  this.name = Text.readString(in);
		  this.age = in.readInt();
		  this.departmentID = in.readInt();
		 }
		
		 //����һ��write()����
		 public void write(DataOutput out) throws IOException {
			 // TODO Auto-generated method stub
			 //writeInt()д������
			 out.writeInt(this.id);
			 
			 //writeString()д���ַ���
             Text.writeString(out, this.name);
             out.writeInt(this.age);
             out.writeInt(this.departmentID);
			}
			
		//����һ��readFields()����
		public void readFields(ResultSet result) throws SQLException {
			// TODO Auto-generated method stub
			this.id = result.getInt(1);
			this.name = result.getString(2);
			this.age = result.getInt(3);
			this.departmentID = result.getInt(4);
		}
		
		//��д write����������ʵ��д�����
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
