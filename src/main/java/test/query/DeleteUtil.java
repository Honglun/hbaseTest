package test.query;

import java.io.FileInputStream;
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

public class DeleteUtil {
	
	static byte[] KEY_DELIMITER = new byte[]{0};
	static Configuration conf=null;
	static Connection connection = null;
	static {
	//classpath
		conf=HBaseConfiguration.create();
		
		try {
			conf.addResource(new FileInputStream("/etc/hbase/conf.cloudera.hbase/hbase-site.xml"));
			conf.addResource(new FileInputStream("/etc/hbase/conf.cloudera.hbase/hdfs-site.xml"));
			connection = ConnectionFactory.createConnection(conf);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	public static void main(String[] agrs){
		String tableName="";
		String rowKey="";
		String family="";
		String column="";
		
		if(agrs.length<2){
			System.out.println("arguments error");
			System.out.println("Useage: ");
			System.out.println("java -jar example.jar tableName rowKey [family [column] ]");
			return;
		}
		
		tableName=agrs[0];
		rowKey=agrs[1];
		
		try {
			if(agrs.length==2){
				DeleteUtil.deleteRow(tableName, rowKey.getBytes());
			}
			
			if(agrs.length==3){
				family=agrs[2];
				DeleteUtil.deleteFamily(tableName, rowKey.getBytes(), family.getBytes());
			}
			if(agrs.length==4){
				family=agrs[2];
				column=agrs[3];
				DeleteUtil.deleteColumn(tableName, rowKey.getBytes(), family.getBytes(), column.getBytes());
			}
		} catch (Exception e) {
			e.printStackTrace();
		}

	}
	
	public static void deleteColumn(String tableName,byte[] rowKey,byte[] family,byte[] qualifier) throws Exception{
		Table table = connection.getTable(TableName.valueOf(tableName));
		Delete delete = new Delete(rowKey);
		delete.setAttribute("tephra.tx.rollback", "1".getBytes());
		delete.addColumn(family, qualifier);
		table.delete(delete);
		table.close();
	}
	
	public static void deleteFamily(String tableName,byte[] rowKey,byte[] family) throws Exception{
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


}
