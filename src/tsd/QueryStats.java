/**
 * Copyright (C) 2015 Turn Inc. All Rights Reserved.
 * Proprietary and confidential.
 */
package net.opentsdb.tsd;

import com.codahale.metrics.Counter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import net.opentsdb.stats.StatsCollector;

public class QueryStats {

	static final MetricRegistry QUERY_METRICS_REGISTRY = new MetricRegistry();

	static final String SCANNED_POINTS = "scannedPoints";

	public static Counter numberOfScannedPointsCounter() {
		return QUERY_METRICS_REGISTRY.counter(SCANNED_POINTS);
	}

	public static Timer groupByTimer() {
		return QUERY_METRICS_REGISTRY.timer("groupByTimer");
	}

	public static Counter numberOfPointsInResponse() {
		return QUERY_METRICS_REGISTRY.counter("responsePoints");
	}

	public static Counter numberOfResponsePointsSerialized() {
		return QUERY_METRICS_REGISTRY.counter("responsePointsSerialized");
	}

	public static Timer processScan() {
		return QUERY_METRICS_REGISTRY.timer("processScan");
	}

	public static Timer downSampleTimer() {
		return QUERY_METRICS_REGISTRY.timer("downSampleTimer");
	}

	public static void collectStats(StatsCollector collector) {
		collector.record("query.scan.output", numberOfScannedPointsCounter().getCount());

		collector.record("query.response.input", numberOfPointsInResponse().getCount());
		collector.record("query.response.serialized", numberOfResponsePointsSerialized().getCount());

		collector.record("query.groupbyTimer.max", groupByTimer().getSnapshot().getMax());
		collector.record("query.groupbyTimer.min", groupByTimer().getSnapshot().getMin());
		collector.record("query.groupbyTimer.mean", groupByTimer().getSnapshot().get75thPercentile());
		collector.record("query.groupbyTimer.75thpercentile", groupByTimer().getSnapshot().get75thPercentile());
		collector.record("query.groupbyTimer.95thpercentile", groupByTimer().getSnapshot().get98thPercentile());
		collector.record("query.groupbyTimer.98thpercentile", groupByTimer().getSnapshot().get98thPercentile());
		collector.record("query.groupbyTimer.99thpercentile", groupByTimer().getSnapshot().get99thPercentile());

		collector.record("query.processScan.mean", processScan().getSnapshot().getMean());
		collector.record("query.processScan.max", processScan().getSnapshot().getMax());
		collector.record("query.processScan.min", processScan().getSnapshot().getMin());
		collector.record("query.processScan.75thpercentile", processScan().getSnapshot().get75thPercentile());
		collector.record("query.processScan.95thpercentile", processScan().getSnapshot().get95thPercentile());
		collector.record("query.processScan.98thpercentile", processScan().getSnapshot().get98thPercentile());
		collector.record("query.processScan.99thpercentile", processScan().getSnapshot().get99thPercentile());

		collector.record("query.downSampleTimer.max", downSampleTimer().getSnapshot().getMax());
		collector.record("query.downSampleTimer.min", downSampleTimer().getSnapshot().getMin());
		collector.record("query.downSampleTimer.mean", downSampleTimer().getSnapshot().get75thPercentile());
		collector.record("query.downSampleTimer.75thpercentile", downSampleTimer().getSnapshot().get75thPercentile());
		collector.record("query.downSampleTimer.95thpercentile", downSampleTimer().getSnapshot().get98thPercentile());
		collector.record("query.downSampleTimer.98thpercentile", downSampleTimer().getSnapshot().get98thPercentile());
		collector.record("query.downSampleTimer.99thpercentile", downSampleTimer().getSnapshot().get99thPercentile());

	}


}
