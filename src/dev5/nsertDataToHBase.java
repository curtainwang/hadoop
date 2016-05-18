public class InsertDataToHBase {
	public static class InsertDataToHBaseMapper extends Mapper<Object, Text, NullContext, NullWritable> {
		public static String table1[] = { "field1", "field2", "field3"};
		public static String table2[] = { "field1", "field2", "field3"};
		public static String table3[] = { "field1", "field2", "field3"};
		public static HTable table = null;
		protected void setup(Context context) throws IOException, InterruptedException {
			HBaseConfiguration conf = new HBaseConfiguration();
			String table_name = context.getConfiguration().get("tabel_name");
			if (table == null) {
				table = new HTable(conf, table_name);
			}
		}

		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			String arr_value[] = value.toString().split("\t");
			String table_name = context.getConfiguration().get("tabel_name");
			String temp_arr[] = table1;
			int temp_value_length = 0;
			if (table_name.trim().equals("table1")) {
				temp_arr = table1;
				temp_value_length = 3;
			} else if (table_name.trim().equals("table2")) {
				temp_arr = table2;
				temp_value_length = 3;
			} else if (table_name.trim().equals("table3")) {
				temp_arr = table3;
				temp_value_length = 3;
			}
			List<Put> list = new ArrayList<Put>();
			if (arr_value.length == temp_value_length) {
				String rowname = System.currentTimeMillis() / 1000 + "" + CommUtil.getSixRadom();
				Put p = new Put(Bytes.toBytes(rowname));
				for (int i = 0; i < temp_arr.length; i++) {
					p.add(temp_arr[i].getBytes(), "".getBytes(), arr_value[i].getBytes());
				}
				list.add(p);
			}
			table.put(list);
			table.flushCommits();
		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		if (otherArgs.length != 3) {
			System.err.println("Usage: InsertDataToHBase <inpath> <outpath> <tablename>");
			System.exit(2);
		}
		conf.set("tabel_name", otherArgs[2]);
		Job job = new Job(conf, "InsertDataToHBase");
		job.setNumReduceTasks(0);
		job.setJarByClass(InsertDataToHBase.class);
		job.setMapperClass(InsertDataToHBaseMapper.class);
		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
		// job.submit();
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
