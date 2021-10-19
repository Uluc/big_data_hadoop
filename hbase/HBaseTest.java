package hbase;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

public class HBaseTest {

	public static void main(String[] args) throws IOException{
		Configuration config= HBaseConfiguration.create();
		Connection connection= ConnectionFactory.createConnection(config);
		Table table= connection.getTable(TableName.valueOf("lsu"));
		try{
			Get get= new Get(Bytes.toBytes("AAA"));
			Result result= table.get(get);
			byte[] value= result.getValue(Bytes.toBytes("info"), Bytes.toBytes("hometown"));
			System.out.println("Get(info:hometown): "+ Bytes.toString(value));
			} finally{
				table.close();
				connection.close();
			}
		}


}