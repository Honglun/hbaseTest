import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.math.BigInteger;

import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableExistsException;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.util.Bytes;

public class Test {
	//git not work
	public static void main(String[] args) throws UnsupportedEncodingException {
		long l = 1234567890L;
		byte[] lb = Bytes.toBytes(l);    
		System.out.println("long bytes length: " + lb.length);   // returns 8

		String s = "" + l;
		byte[] sb = s.getBytes("utf-8");
		System.out.println("long as string length: " + sb.length);    // returns 10
		
		String str=String.format("%016x", 16);//00010000   %:占位符，0：若内容长度不足最小宽度，则在左边用0来填充,16:宽度，x：十六进制
		System.out.println(str);
	}
	
	public static boolean createTable(HBaseAdmin admin, HTableDescriptor table, byte[][] splits) throws IOException {
		try {
			//admin.createTable(desc, startKey, endKey, numRegions);
			admin.createTable(table, splits);
			return true;
		} catch (TableExistsException e) {
			System.out.println("table " + table.getNameAsString() + " already exists");
			// the table already exists...
			return false;
		}
	}

	public static byte[][] getHexSplits(String startKey, String endKey, int numRegions) {
		byte[][] splits = new byte[numRegions - 1][];
		BigInteger lowestKey = new BigInteger(startKey, 16);
		BigInteger highestKey = new BigInteger(endKey, 16);
		BigInteger range = highestKey.subtract(lowestKey);
		BigInteger regionIncrement = range.divide(BigInteger.valueOf(numRegions));
		lowestKey = lowestKey.add(regionIncrement);
		for (int i = 0; i < numRegions - 1; i++) {
			BigInteger key = lowestKey.add(regionIncrement.multiply(BigInteger.valueOf(i)));
			byte[] b = String.format("%016x", key).getBytes();
			splits[i] = b;
		}
		return splits;
	}

}
