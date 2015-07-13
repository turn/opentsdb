/**
 * Copyright (C) 2015 Turn Inc. All Rights Reserved.
 * Proprietary and confidential.
 */
package net.opentsdb.core;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.concurrent.TimeUnit;

import com.stumbleupon.async.Callback;
import net.opentsdb.utils.Config;
import org.hbase.async.Bytes;
import org.hbase.async.KeyValue;
import org.hbase.async.Scanner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HBaseStateChecker {

	private static final Logger LOG = LoggerFactory.getLogger(HBaseStateChecker.class);

	public static void main(String[] args) throws Exception {

		long start_time = System.currentTimeMillis() - TimeUnit.MILLISECONDS.convert(1000, TimeUnit.MINUTES);
		long end_time = System.currentTimeMillis();

		final TSDB tsdb = new TSDB(new Config("src/opentsdb.conf"));

		int metric_width = tsdb.metrics().width();
		byte[] metricId = tsdb.metrics().getId("proc.stat.cpu.percpu");
		LOG.info("id={}, width={}", Arrays.toString(metricId), metric_width);

		final byte[] start_row = new byte[metric_width + Const.TIMESTAMP_BYTES];
		final byte[] end_row = new byte[metric_width + Const.TIMESTAMP_BYTES];

		System.arraycopy(metricId, 0, start_row, 0, metric_width);
		System.arraycopy(metricId, 0, end_row, 0, metric_width);

		Bytes.setInt(start_row, (int) getScanStartTimeSeconds(start_time), metric_width);
		Bytes.setInt(end_row, (int) getScanEndTimeSeconds(end_time), metric_width);

		Scanner scanner = tsdb.getClient().newScanner(tsdb.dataTable());
//		scanner.setMaxNumRows(Scanner.DEFAULT_MAX_NUM_ROWS * 4);
		scanner.setStartKey(start_row);
		scanner.setStopKey(end_row);

		new ScannerCB(scanner, tsdb).scan();

		Thread.sleep(20000);
	}

	// start can be in millis or seconds. values in millis will be converted to seconds
	private static long getScanStartTimeSeconds(long start) {
		if ((start & Const.SECOND_MASK) != 0) {
			start /= 1000;
		}
		final long ts = start - Const.MAX_TIMESPAN * 2;
		return ts > 0 ? ts : 0;
	}

	// end can be in millis or seconds. values in millis will be converted to seconds
	private static long getScanEndTimeSeconds(long end) {
		if ((end & Const.SECOND_MASK) != 0) {
			end /= 1000;
		}
		return end + Const.MAX_TIMESPAN + 1;
	}

	static class ScannerCB implements Callback<Object,
			ArrayList<ArrayList<KeyValue>>> {

		private final Scanner scanner;
		private final TSDB tsdb;

		public ScannerCB(Scanner scanner, TSDB tsdb) {
			this.scanner = scanner;
			this.tsdb = tsdb;
		}

		public void scan() {
			scanner.nextRows().addCallback(this);
		}

		@Override
		public Object call(ArrayList<ArrayList<KeyValue>> rows) throws Exception {
			if (rows != null) {
				Thread.sleep(100);
				LOG.info("Obtained {} rows", rows.size());
				scan();
			} else {
				LOG.info("Done");
				scanner.close().joinUninterruptibly();
				tsdb.shutdown().joinUninterruptibly();
			}
			return null;
		}
	}
}
