package com.xuwuji.hadoop.hbase.client;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;

import com.xuwuji.hadoop.util.TimeUtil;

public class Client {

	public static void insert() throws IOException {
		HTable table = null;
		Configuration conf = HBaseConfiguration.create();
		try {
			table = new HTable(conf, "test");
			byte[] family = Bytes.toBytes("n");
			for (int i = 0; i < 10; i++) {
				String rowKey = String.valueOf(i);
				Put p = new Put(Bytes.toBytes(rowKey));
				long ts = System.currentTimeMillis();
				for (int j = 0; j < 10; j++) {
					String qualifier = String.valueOf(j);
					String value = String.valueOf("data : " + j);
					p.addColumn(family, Bytes.toBytes(qualifier), ts, Bytes.toBytes(value));
				}
				table.put(p);
			}
		} finally {
			table.close();
		}
	}

	public static void select() throws IOException {
		Configuration conf = HBaseConfiguration.create();
		// Get table instance
		HTable table = new HTable(conf, "test");
		// Create Get with rowkey
		Get get = new Get(Bytes.toBytes("1"));
		Result result = table.get(get);
		byte[] val = result.getValue(Bytes.toBytes("n"), Bytes.toBytes("1"));
		System.out.println("Cell Value: " + Bytes.toString(val));
		table.close();
	}

	public static void scan() throws IOException {
		// Get instance of Default Configuration
		Configuration conf = HBaseConfiguration.create();
		// Get table instance
		HTable table = new HTable(conf, "test");

		// Create Scan instance
		Scan scan = new Scan();

		// Add a column with value "Hello", in ìcf1:greetî, to the Put.
		scan.addColumn(Bytes.toBytes("n"), Bytes.toBytes("1"));

		// Set Start Row
		// scan.setStartRow(Bytes.toBytes("1"));

		// Set End Row
		// scan.setStopRow(Bytes.toBytes("10"));

		// Get Scanner Results
		ResultScanner scanner = table.getScanner(scan);

		for (Result res : scanner) {
			System.out.println("Row Value: " + res);
		}
		scanner.close();
		table.close();
	}

	@SuppressWarnings({ "deprecation" })
	public static void main(String[] args) throws IOException {
		// insert();
		// select();
		scan();
	}

}
