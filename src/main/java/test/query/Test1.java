package test.query;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map.Entry;
import java.util.NavigableMap;
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
		String tableName="";
		String rowKey="d6df3475fa109370b44447ea43914e753b57dcb2";
		String family="";
		String column="";
		if(agrs.length==2){
			
		}

		
		try {
			//cdap_assuredplus:OaxisOnBoardDeviceDataSet
			tableName = "cdap_assuredplus:OaxisOnBoardDeviceDataSet";
			// HBaseTestCase.creatTable(tablename);
			// Test1.addData(tablename);
			//Test1.creatTable(tableName);
			// Test1.getData(tableName);
			//Test1.getAllData(tableName);
			// Test1.deleteData(tableName);
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
			Test1.deleteRow(tableName, rowKey.getBytes());
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
		//8296746667235794835 1508827576941000000
		//10123352874082195
		Table table = connection.getTable(TableName.valueOf(tableName));
		Get get = new Get(Bytes.toBytes(10123352874082195l));
		Result rs = table.get(get);
//		NavigableMap<byte[], byte[]> familyMap = rs.getFamilyMap("q".getBytes());
//		for(Entry<byte[], byte[]> e:familyMap.entrySet()){
//			System.out.println(Bytes.toLong(e.getKey())+" "+Bytes.toLong(e.getValue()));
//		}
		String value = new String(rs.getValue("q".getBytes(), Bytes.toBytes(8296746667235794835l)));
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
		
		//List<Delete> deletes=new ArrayList<>();
		Scan s = new Scan();
		//s.setFilter();
		ResultScanner rs = table.getScanner(s);

		for (Result r = rs.next(); r != null; r = rs.next()) {
			byte[] rowB = r.getRow();
			byte[] cloumnB=r.getValue("d".getBytes(), "subAccountEmail".getBytes());
			NavigableMap<byte[], byte[]> familyMap = r.getFamilyMap("d".getBytes());
			
			if(cloumnB!=null){
				//System.out.println(new String(rowB)+"   "+ new String(cloumnB));
				//System.out.println();
			}else{
				//System.out.println(rowB);
				//System.out.println(deletes);
//				System.out.println(new String(rowB));
				Delete delete = new Delete(rowB);
				delete.setAttribute("tephra.tx.rollback", "1".getBytes());
				//delete.setAttribute("cask.tx.rollback", "1".getBytes());
				for(byte[] qualifier: familyMap.keySet()){
					//System.out.println("column "+new String(qualifier)+" value "+new String(familyMap.get(qualifier)));
					delete.addColumn("d".getBytes(), qualifier);
				}
				
				//table.delete(delete);
				//delete.addColumn("d".getBytes());
				
				//System.out.println(new String(rowB)+" delete data ok .");
			}
			
			
		}
		table.close();
		//System.out.println(deletes);
		//Table tab2 = connection.getTable(TableName.valueOf(tablename));
		//tab2.delete(deletes);
		//tab2.close();
	}
	
	
	public void deleteColumn(String tableName,byte[] rowKey,byte[] family,byte[] qualifier) throws Exception{
		Table table = connection.getTable(TableName.valueOf(tableName));
		Delete delete = new Delete(rowKey);
		delete.setAttribute("tephra.tx.rollback", "1".getBytes());
		delete.addColumn(family, qualifier);
		table.delete(delete);
		table.close();
	}
	
	public void deleteFamily(String tableName,byte[] rowKey,byte[] family) throws Exception{
		Table table = connection.getTable(TableName.valueOf(tableName));
		Result result = table.get(new Get(rowKey));
		Delete delete = new Delete(rowKey);
		delete.setAttribute("tephra.tx.rollback", "1".getBytes());
		NavigableMap<byte[], byte[]> familyMap = result.getFamilyMap(family);
		for(byte[] qualifier: familyMap.keySet()){
			delete.addColumns(family, qualifier);
		}
		table.delete(delete);
		table.close();
	}
	
	public static void deleteRow(String tableName,byte[] rowKey) throws Exception{
		Table table = connection.getTable(TableName.valueOf(tableName));
		Result result = table.get(new Get(rowKey));
		Delete delete = new Delete(rowKey);
		delete.setAttribute("tephra.tx.rollback", "1".getBytes());
		NavigableMap<byte[], NavigableMap<byte[], NavigableMap<Long, byte[]>>> rowMap = result.getMap();
		for(byte[] f:rowMap.keySet()){
			Set<byte[]> qualifierSet = rowMap.get(f).keySet();
			for(byte[] qualifier: qualifierSet){
				delete.addColumns(f, qualifier);
			}
		}
		table.delete(delete);
		table.close();
	}

	public static void deleteData(String tablename) throws IOException {
		Table table = connection.getTable(TableName.valueOf(tablename));
		Delete delete = new Delete("903d4ba7d12169b70a94ca0caa621c36e590c921".getBytes());
		//delete.addColumn("basic_info".getBytes(), "name".getBytes());
		// delete column, if no column , will delete the whole row
		// delete.deleteColumn("basic_info".getBytes(), "name".getBytes());
		delete.setAttribute("tephra.tx.rollback", "1".getBytes());
		delete.addColumn("d".getBytes(), "HostDeviceUniqueId".getBytes());
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
