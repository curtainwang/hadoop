import java.io.IOException;   
import java.io.ByteArrayOutputStream;   
import java.io.DataOutputStream;   
import java.io.ByteArrayInputStream;   
import java.io.DataInputStream;   
import java.util.Map;   
import org.apache.hadoop.io.Writable;   
import org.apache.hadoop.io.IntWritable;   
import org.apache.hadoop.hbase.HBaseConfiguration;   
import org.apache.hadoop.hbase.HTableDescriptor;   
import org.apache.hadoop.hbase.HColumnDescriptor;   
import org.apache.hadoop.hbase.client.HBaseAdmin;   
import org.apache.hadoop.hbase.client.HTable;   
import org.apache.hadoop.hbase.io.BatchUpdate;   
import org.apache.hadoop.hbase.io.RowResult;   
import org.apache.hadoop.hbase.io.Cell;   
import org.apache.hadoop.hbase.util.Writables;   

public class HBaseBasic {   
    public static void main(String[] args) throws Exception {   
        HBaseConfiguration config = new HBaseConfiguration();   
        HBaseAdmin admin = new HBaseAdmin(config);   
        if (admin.tableExists("scores")) {   
            System.out.println("drop table");   
  
            admin.disableTable("scores");   
            admin.deleteTable("scores");   
        }   
        System.out.println("create table");   
        HTableDescriptor tableDescripter = new HTableDescriptor("scores".getBytes());   
        tableDescripter.addFamily(new HColumnDescriptor("grade:"));   
        tableDescripter.addFamily(new HColumnDescriptor("course:"));   
        admin.createTable(tableDescripter);   
        HTable table = new HTable(config, "scores");   
        System.out.println("add Tom's data");   
        BatchUpdate tomUpdate = new BatchUpdate("Tom");   
        tomUpdate.put("grade:", Writables.getBytes(new IntWritable(1)));   
        tomUpdate.put("course:math", Writables.getBytes(new IntWritable(87)));   
        tomUpdate.put("course:art", Writables.getBytes(new IntWritable(97)));   
        table.commit(tomUpdate);   
        System.out.println("add Jerry's data");   
        BatchUpdate jerryUpdate = new BatchUpdate("Jerry");   
        jerryUpdate.put("grade:", Writables.getBytes(new IntWritable(2)));   
        jerryUpdate.put("course:math", Writables.getBytes(new IntWritable(100)));   
        jerryUpdate.put("course:art", Writables.getBytes(new IntWritable(80)));   
        table.commit(jerryUpdate);   
        for (RowResult row : table.getScanner(new String[] { "course:" })) {   
   System.out.format("ROW\t%s\n", new String(row.getRow()));   
            for (Map.Entry<byte[], Cell> entry : row.entrySet()) {   
                String column = new String(entry.getKey());   
                Cell cell = entry.getValue();   
                IntWritable value = new IntWritable();   
                Writables.copyWritable(cell.getValue(), value);   
                System.out.format("  COLUMN\t%s\t%d\n", column, value.get());   
  
            }   
        }   
    }   
}   
