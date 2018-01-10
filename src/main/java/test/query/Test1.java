package test.query;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.util.Bytes;

public class Test1 {
	
	static Configuration conf=null;
	static Connection connection = null;
	static {
	//classpath
		conf=HBaseConfiguration.create();
		try {
			connection = ConnectionFactory.createConnection(conf);
		} catch (IOException e) {
			e.printStackTrace();
		}
	//cfg.addResource("ch2/hbase-site2.xml");
	}
	
	
	public static void main(String[] agrs) {
		
//		int i=0xFF;
//		byte b=(byte)i;
//		System.out.println((b));
		
		try {
			String tablename = "teacher";
			// HBaseTestCase.creatTable(tablename);
			// Test1.addData(tablename);
			//Test1.creatTable(tablename);
			// Test1.getData(tablename);
			//Test1.getAllData(tablename);
			// Test1.deleteData(tablename);
			Test1.scan(tablename);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	public static void creatTable(String tablename) throws Exception {
		Admin admin =  connection.getAdmin();
		if (admin.tableExists(TableName.valueOf(tablename))) {
			System.out.println("table Exists!!!");
		} else {
			HTableDescriptor tableDesc = new HTableDescriptor(TableName.valueOf(tablename));
			// tableDesc.set
			tableDesc.addFamily(new HColumnDescriptor("basic_info"));
			admin.createTable(tableDesc);
			System.out.println("create table ok .");
		}
	}

	public static void addData(String tablename) throws Exception {
		Table table = connection.getTable(TableName.valueOf(tablename));
		KeyValue kv = new KeyValue("row@ccccc".getBytes(), "basic_info".getBytes(), "name".getBytes(), "xiaoh".getBytes());
		Put put = new Put("row@ccccc".getBytes());
		put.add(kv);
		table.put(put);
		System.out.println("add data ok .");
	}
	
	
	public static void getData(String tableName) throws IOException {
		Table table = connection.getTable(TableName.valueOf(tableName));
		Get get = new Get("row1".getBytes());
		Result rs = table.get(get);
		String value = new String(rs.getValue("basic_info".getBytes(), "name".getBytes()));
		System.out.println(value);
	}

	public static void getAllData(String tablename) throws Exception {
		Table table = connection.getTable(TableName.valueOf(tablename));
		Scan s = new Scan();
		ResultScanner rs = table.getScanner(s);

		for (Result r = rs.next(); r != null; r = rs.next()) {
			byte[] rowB = r.getRow();
			System.out.println(new String(rowB));
		}
	}

	public static void deleteData(String tablename) throws IOException {
		Table table = connection.getTable(TableName.valueOf(tablename));
		Delete delete = new Delete("row1".getBytes());
		delete.addColumn("basic_info".getBytes(), "name".getBytes());
		// delete column, if no column , will delete the whole row
		// delete.deleteColumn("basic_info".getBytes(), "name".getBytes());
		table.delete(delete);
		System.out.println("delete data ok .");
	}
	
	public static void scan(String tablename) throws Exception {
		Table table = connection.getTable(TableName.valueOf(tablename));
		Scan s = new Scan();
		s.addFamily("basic_info".getBytes());
		//s.setFilter();
		//s.setRowPrefixFilter("row".getBytes());
		s.setStartRow("row1".getBytes());
		s.setStopRow(new byte [0]);
		ResultScanner rs = table.getScanner(s);

		for (Result r = rs.next(); r != null; r = rs.next()) {
			byte[] rowB = r.getRow();
			System.out.println(new String(rowB));
		}
	}


}
