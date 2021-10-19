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

public class HBasePut2 {

	public static void main(String[] args) throws IOException {
		Configuration config = HBaseConfiguration.create();
		Connection connection = ConnectionFactory.createConnection(config);
		Table table = connection.getTable(TableName.valueOf("courses"));

		try {
			Put put = new Put(Bytes.toBytes("CS3501"));
			byte[] columnFamily = Bytes.toBytes("info");
			byte[] qualifier = Bytes.toBytes("name");
			byte[] value = Bytes.toBytes("comp. org.");
			put.addImmutable(columnFamily, qualifier, value);
			qualifier = Bytes.toBytes("credit");
			value = Bytes.toBytes("3");
			put.addImmutable(columnFamily, qualifier, value);
			table.put(put);
			
			Put put2 = new Put(Bytes.toBytes("CS4740"));
			qualifier = Bytes.toBytes("name");
			value = Bytes.toBytes("big data");
			put2.addImmutable(columnFamily, qualifier, value);
			qualifier = Bytes.toBytes("credit");
			value = Bytes.toBytes("3");
			put2.addImmutable(columnFamily, qualifier, value);
			qualifier = Bytes.toBytes("since");
			value = Bytes.toBytes("2015");
			put2.addImmutable(columnFamily, qualifier, value);
			columnFamily = Bytes.toBytes("instructors");
			qualifier = Bytes.toBytes("2015S");
			value = Bytes.toBytes("Wang");
			put2.addImmutable(columnFamily, qualifier, value);
			qualifier = Bytes.toBytes("2021S");
			value = Bytes.toBytes("Lee");
			put2.addImmutable(columnFamily, qualifier, value);
			table.put(put2);
			
			Put put3 = new Put(Bytes.toBytes("CS4998"));
			columnFamily = Bytes.toBytes("info");
			qualifier = Bytes.toBytes("credit");
			value = Bytes.toBytes("1");
			put3.addImmutable(columnFamily, qualifier, value);
			table.put(put3);
			
			Put put4 = new Put(Bytes.toBytes("CS7402"));
			qualifier = Bytes.toBytes("name");
			value = Bytes.toBytes("DB");
			put4.addImmutable(columnFamily, qualifier, value);
			qualifier = Bytes.toBytes("credit");
			value = Bytes.toBytes("3");
			put4.addImmutable(columnFamily, qualifier, value);
			columnFamily = Bytes.toBytes("instructors");
			qualifier = Bytes.toBytes("2021S");
			value = Bytes.toBytes("TBA");
			put4.addImmutable(columnFamily, qualifier, value);
			table.put(put4);
			
		} finally {
			table.close();
			connection.close();
		}

	}

}
