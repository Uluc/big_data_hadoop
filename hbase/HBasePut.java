package hbase;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

public class HBasePut {

	public static void main(String[] args) throws IOException {
		Configuration config = HBaseConfiguration.create();
		Connection connection = ConnectionFactory.createConnection(config);
		Table table = connection.getTable(TableName.valueOf("courses"));

		try {
			Put put = new Put(Bytes.toBytes("CS4999"));
			byte[] columnFamily = Bytes.toBytes("info");
			byte[] qualifier = Bytes.toBytes("name");
			byte[] value = Bytes.toBytes("test");
			put.addImmutable(columnFamily, qualifier, value);
			table.put(put);
		} finally {
			table.close();
			connection.close();
		}

	}

}
