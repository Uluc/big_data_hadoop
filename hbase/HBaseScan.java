package hbase;

import java.io.IOException;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

public class HBaseScan {

	public static void main(String[] args) throws IOException {
		Configuration config = HBaseConfiguration.create();
		Connection connection = ConnectionFactory.createConnection(config);
		Table table = connection.getTable(TableName.valueOf("courses"));

		try {
			Scan scan = new Scan(Bytes.toBytes("CS4000"), Bytes.toBytes("CS5000"));
			ResultScanner scanner = table.getScanner(scan);
			try {
				for(Result result : scanner) {
					System.out.println("row key: " + Bytes.toString(result.getRow()));
					Map<byte[], byte[]> infoMap = result.getFamilyMap(Bytes.toBytes("info"));
					for(byte[] column : infoMap.keySet())
						System.out.println("\t" + Bytes.toString(column) + " => " + Bytes.toString(infoMap.get(column)));
				}
			} finally {
				scanner.close();
			}
		} finally {
			table.close();
			connection.close();
		}

	}

}
