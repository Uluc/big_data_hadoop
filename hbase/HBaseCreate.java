package hbase;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;

public class HBaseCreate {

	public static void main(String[] args) throws IOException {
		Configuration config = HBaseConfiguration.create();
		Connection connection = ConnectionFactory.createConnection(config);
		Admin admin = connection.getAdmin();
		try {
			HTableDescriptor htd = new HTableDescriptor(TableName.valueOf("courses"));
			HColumnDescriptor hcd1 = new HColumnDescriptor("info");
			HColumnDescriptor hcd2 = new HColumnDescriptor("instructors");
			htd.addFamily(hcd1);
			htd.addFamily(hcd2);
			admin.createTable(htd);
		} finally {
			admin.close();
			connection.close();
		}
	}

}
