package test.query;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

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
import org.apache.hadoop.hbase.filter.FirstKeyValueMatchingQualifiersFilter;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Test;
import org.junit.runners.Parameterized.Parameters;

import test.bean.RowData;

public class Test1 {
	
	static byte[] KEY_DELIMITER = new byte[]{0};
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
			String tableName = "test_s";
			// HBaseTestCase.creatTable(tablename);
			// Test1.addData(tablename);
			Test1.creatTable(tableName);
			// Test1.getData(tablename);
			//Test1.getAllData(tablename);
			// Test1.deleteData(tablename);
			//Test1.scan(tablename);
			//Test1.addIndexTable();
//			byte[] i=new byte[]{01};
//			System.out.println(new String(i));
			//Test1.findByColumn(tablename, "name".getBytes(), "xiaohong".getBytes());
//			RowData rowData=new RowData();
//			rowData.setRowKey(Bytes.toBytes(20));
//			rowData.setFamily("f1".getBytes());
//			rowData.setQualifier("name".getBytes());
//			rowData.setValue("xiaoh".getBytes());
//			Test1.addData(tableName, rowData);
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
			tableDesc.addFamily(new HColumnDescriptor("basic_info"));
			//admin.createTable(tableDesc);
			byte[][] splitKeys=new byte[4][];
			for(int i=0;i<4;i++){
				splitKeys[i]=Bytes.toBytes((i+1)*20);
			}
			admin.createTable(tableDesc, splitKeys);
			//admin.set
			System.out.println("create table ok .");
		}
	}

	public static void addData(String tablename) throws Exception {
		Table table = connection.getTable(TableName.valueOf(tablename));
		byte[] rowKey=Bytes.add("row@eeeee_".getBytes(), Bytes.toBytes(0));
		KeyValue kv = new KeyValue(rowKey, "basic_info".getBytes(), "name".getBytes(), "xiaoh".getBytes());
		Put put = new Put(rowKey);
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
	
	public static void getData(String tableName,RowData rowData) throws IOException {
		Table table = connection.getTable(TableName.valueOf(tableName));
		Get get = new Get(rowData.getRowKey());
		Result rs = table.get(get);
		String value = new String(rs.getValue(rowData.getFamily(), rowData.getQualifier()));
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
	
	public static List<byte[]> scan(String tablename, RowData rowData) throws Exception {
		List<byte[]> rowKeys=new ArrayList<byte[]>();
		Table table = connection.getTable(TableName.valueOf(tablename));
		Scan s = new Scan();
		s.addFamily(rowData.getFamily());
		//s.setFilter();
		s.setRowPrefixFilter(rowData.getRowKey());
		//s.setStartRow("row1".getBytes());
		//s.setStopRow(new byte [0]);
//		Set<byte[]> set = new HashSet<byte[]>();
//		set.add("name1".getBytes());
//		Filter filter = new FirstKeyValueMatchingQualifiersFilter(set);
//		s.setFilter(filter);
		
		ResultScanner rs = table.getScanner(s);

		for (Result r = rs.next(); r != null; r = rs.next()) {
			byte[] rowB = r.getRow();
			byte[] cloumnB=r.getValue(rowData.getFamily(), rowData.getQualifier());
			System.out.println(new String(rowB)+"   "+ new String(cloumnB));
			rowKeys.add(cloumnB);
		}
		return rowKeys;
	}
	
	public static void addIndexTable() throws Exception{
		RowData tableRow=new RowData();
		byte[] rowKey="tableRow1".getBytes();
		byte[] column="name".getBytes();
		byte[] value="xiaohong".getBytes();
		
		tableRow.setRowKey(rowKey);
		tableRow.setFamily("basic_info".getBytes());
		tableRow.setQualifier(column);
		tableRow.setValue(value);
		
		RowData indexRow=new RowData();
		indexRow.setRowKey(createIndexKey(rowKey,column,value));
		indexRow.setFamily("value".getBytes());
		indexRow.setQualifier("v".getBytes());
		//store rowKey
		indexRow.setValue(rowKey);
		addData("test",tableRow);
		addData("test.i",indexRow);
	}
	
	public static void findByColumn(String tableName,byte[] columnName, byte[] columnValue) throws Exception{
		String indexTabName=tableName+".i";
		//get rowKey
		RowData indexRow=new RowData();
		indexRow.setRowKey(createIndexKey(columnName,columnValue));
		indexRow.setFamily("value".getBytes());
		indexRow.setQualifier("v".getBytes());
		List<byte[]> rowKeys=scan(indexTabName, indexRow);
		System.out.println(rowKeys);
		for(byte[] rowKey:rowKeys){
			RowData tableRow=new RowData();
			tableRow.setRowKey(rowKey);
			tableRow.setFamily("basic_info".getBytes());
			tableRow.setQualifier(columnName);
			getData(tableName, tableRow);
		}
	}
	
	public static void addData(String tableName,RowData rowData) throws Exception {
		Table table = connection.getTable(TableName.valueOf(tableName));
		KeyValue kv = new KeyValue(rowData.getRowKey(), rowData.getFamily(), rowData.getQualifier(), rowData.getValue());
		Put put = new Put(rowData.getRowKey());
		put.add(kv);
		table.put(put);
		System.out.println("add data ok .");
	}
	
	private static byte[] createIndexKey(byte[] row, byte[] column, byte[] value) {
		return test.utils.Bytes.concat(column, KEY_DELIMITER, value, KEY_DELIMITER, row);
	}
	
	private static byte[] createIndexKey(byte[] column, byte[] value) {
		return test.utils.Bytes.concat(column, KEY_DELIMITER, value);
	}
	
	


}
