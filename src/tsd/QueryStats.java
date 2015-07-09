/**
 * Copyright (C) 2015 Turn Inc. All Rights Reserved.
 * Proprietary and confidential.
 */
package net.opentsdb.tsd;

import net.opentsdb.core.metrics.Counter;
import net.opentsdb.core.metrics.MetricRegistry;
import net.opentsdb.core.metrics.Timer;
import net.opentsdb.stats.StatsCollector;

public class QueryStats {

	static final MetricRegistry QUERY_METRICS_REGISTRY = new MetricRegistry();

	static final String SCANNED_POINTS = "scannedPoints";

	public static Counter numQueries() {
		return QUERY_METRICS_REGISTRY.counter("numQueries");
	}

	public static Counter numMetrics() {
		return QUERY_METRICS_REGISTRY.counter("numMetrics");
	}

	public static Counter numExpressions() {
		return QUERY_METRICS_REGISTRY.counter("numExpressions");
	}

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

	public static Timer hbaseScan() {
		return QUERY_METRICS_REGISTRY.timer("hbaseScan");
	}

	public static Timer findSpans() {
		return QUERY_METRICS_REGISTRY.timer("findSpans");
	}

	public static Timer downSampleTimer() {
		return QUERY_METRICS_REGISTRY.timer("downSampleTimer");
	}

	public static Timer aggregationTimer() {
		return QUERY_METRICS_REGISTRY.timer("aggregationTimer");
	}

	public static Timer moveToNext() {
		return QUERY_METRICS_REGISTRY.timer("moveToNext");
	}

	public static Timer interpolationTimer() {
		return QUERY_METRICS_REGISTRY.timer("interpolationTimer");
	}

	public static Timer resultProcessing() {
		return QUERY_METRICS_REGISTRY.timer("interpolationTimer");
	}

	public static Timer queryExecutionTimer() {
		return QUERY_METRICS_REGISTRY.timer("queryExecution");
	}

	public static void collectStats(StatsCollector collector) {

		collector.record("query.queries.count", numQueries().getCount());
		collector.record("query.metrics.count", numMetrics().getCount());
		collector.record("query.expressions.count", numExpressions().getCount());

		collector.record("query.scan.output", numberOfScannedPointsCounter().getCount());

		collector.record("query.response.input", numberOfPointsInResponse().getCount());
		collector.record("query.response.serialized", numberOfResponsePointsSerialized().getCount());

		collector.record("query.perCallbackScan.mean", processScan().getSnapshot().getMean());
		collector.record("query.perCallbackScan.max", processScan().getSnapshot().getMax());
		collector.record("query.perCallbackScan.min", processScan().getSnapshot().getMin());
		collector.record("query.perCallbackScan.75thpercentile", processScan().getSnapshot().get75thPercentile());
		collector.record("query.perCallbackScan.95thpercentile", processScan().getSnapshot().get95thPercentile());
		collector.record("query.perCallbackScan.98thpercentile", processScan().getSnapshot().get98thPercentile());
		collector.record("query.perCallbackScan.99thpercentile", processScan().getSnapshot().get99thPercentile());

		collector.record("query.hbaseRead.mean", hbaseScan().getSnapshot().getMean());
		collector.record("query.hbaseRead.max", hbaseScan().getSnapshot().getMax());
		collector.record("query.hbaseRead.min", hbaseScan().getSnapshot().getMin());
		collector.record("query.hbaseRead.75thpercentile", hbaseScan().getSnapshot().get75thPercentile());
		collector.record("query.hbaseRead.95thpercentile", hbaseScan().getSnapshot().get95thPercentile());
		collector.record("query.hbaseRead.98thpercentile", hbaseScan().getSnapshot().get98thPercentile());
		collector.record("query.hbaseRead.99thpercentile", hbaseScan().getSnapshot().get99thPercentile());

		collector.record("query.findSpans.mean", findSpans().getSnapshot().getMean());
		collector.record("query.findSpans.max", findSpans().getSnapshot().getMax());
		collector.record("query.findSpans.min", findSpans().getSnapshot().getMin());
		collector.record("query.findSpans.75thpercentile", findSpans().getSnapshot().get75thPercentile());
		collector.record("query.findSpans.95thpercentile", findSpans().getSnapshot().get95thPercentile());
		collector.record("query.findSpans.98thpercentile", findSpans().getSnapshot().get98thPercentile());
		collector.record("query.findSpans.99thpercentile", findSpans().getSnapshot().get99thPercentile());

		collector.record("query.groupbyTimer.max", groupByTimer().getSnapshot().getMax());
		collector.record("query.groupbyTimer.min", groupByTimer().getSnapshot().getMin());
		collector.record("query.groupbyTimer.mean", groupByTimer().getSnapshot().get75thPercentile());
		collector.record("query.groupbyTimer.75thpercentile", groupByTimer().getSnapshot().get75thPercentile());
		collector.record("query.groupbyTimer.95thpercentile", groupByTimer().getSnapshot().get98thPercentile());
		collector.record("query.groupbyTimer.98thpercentile", groupByTimer().getSnapshot().get98thPercentile());
		collector.record("query.groupbyTimer.99thpercentile", groupByTimer().getSnapshot().get99thPercentile());

		collector.record("query.downSampleTimer.max", downSampleTimer().getSnapshot().getMax());
		collector.record("query.downSampleTimer.min", downSampleTimer().getSnapshot().getMin());
		collector.record("query.downSampleTimer.mean", downSampleTimer().getSnapshot().get75thPercentile());
		collector.record("query.downSampleTimer.75thpercentile", downSampleTimer().getSnapshot().get75thPercentile());
		collector.record("query.downSampleTimer.95thpercentile", downSampleTimer().getSnapshot().get98thPercentile());
		collector.record("query.downSampleTimer.98thpercentile", downSampleTimer().getSnapshot().get98thPercentile());
		collector.record("query.downSampleTimer.99thpercentile", downSampleTimer().getSnapshot().get99thPercentile());

		collector.record("query.interpolationTimer.max", interpolationTimer().getSnapshot().getMax());
		collector.record("query.interpolationTimer.min", interpolationTimer().getSnapshot().getMin());
		collector.record("query.interpolationTimer.mean", interpolationTimer().getSnapshot().get75thPercentile());
		collector.record("query.interpolationTimer.75thpercentile", interpolationTimer().getSnapshot().get75thPercentile());
		collector.record("query.interpolationTimer.95thpercentile", interpolationTimer().getSnapshot().get98thPercentile());
		collector.record("query.interpolationTimer.98thpercentile", interpolationTimer().getSnapshot().get98thPercentile());
		collector.record("query.interpolationTimer.99thpercentile", interpolationTimer().getSnapshot().get99thPercentile());

		collector.record("query.aggregationTimer.max", aggregationTimer().getSnapshot().getMax());
		collector.record("query.aggregationTimer.min", aggregationTimer().getSnapshot().getMin());
		collector.record("query.aggregationTimer.mean", aggregationTimer().getSnapshot().get75thPercentile());
		collector.record("query.aggregationTimer.75thpercentile", aggregationTimer().getSnapshot().get75thPercentile());
		collector.record("query.aggregationTimer.95thpercentile", aggregationTimer().getSnapshot().get98thPercentile());
		collector.record("query.aggregationTimer.98thpercentile", aggregationTimer().getSnapshot().get98thPercentile());
		collector.record("query.aggregationTimer.99thpercentile", aggregationTimer().getSnapshot().get99thPercentile());

		collector.record("query.moveToNextTimer.max", moveToNext().getSnapshot().getMax());
		collector.record("query.moveToNextTimer.min", moveToNext().getSnapshot().getMin());
		collector.record("query.moveToNextTimer.mean", moveToNext().getSnapshot().get75thPercentile());
		collector.record("query.moveToNextTimer.75thpercentile", moveToNext().getSnapshot().get75thPercentile());
		collector.record("query.moveToNextTimer.95thpercentile", moveToNext().getSnapshot().get98thPercentile());
		collector.record("query.moveToNextTimer.98thpercentile", moveToNext().getSnapshot().get98thPercentile());
		collector.record("query.moveToNextTimer.99thpercentile", moveToNext().getSnapshot().get99thPercentile());

		collector.record("query.resultProcessing.max", resultProcessing().getSnapshot().getMax());
		collector.record("query.resultProcessing.min", resultProcessing().getSnapshot().getMin());
		collector.record("query.resultProcessing.mean", resultProcessing().getSnapshot().get75thPercentile());
		collector.record("query.resultProcessing.75thpercentile", resultProcessing().getSnapshot().get75thPercentile());
		collector.record("query.resultProcessing.95thpercentile", resultProcessing().getSnapshot().get98thPercentile());
		collector.record("query.resultProcessing.98thpercentile", resultProcessing().getSnapshot().get98thPercentile());
		collector.record("query.resultProcessing.99thpercentile", resultProcessing().getSnapshot().get99thPercentile());

		collector.record("query.queryExecution.max", queryExecutionTimer().getSnapshot().getMax());
		collector.record("query.queryExecution.min", queryExecutionTimer().getSnapshot().getMin());
		collector.record("query.queryExecution.mean", queryExecutionTimer().getSnapshot().get75thPercentile());
		collector.record("query.queryExecution.75thpercentile", queryExecutionTimer().getSnapshot().get75thPercentile());
		collector.record("query.queryExecution.95thpercentile", queryExecutionTimer().getSnapshot().get98thPercentile());
		collector.record("query.queryExecution.98thpercentile", queryExecutionTimer().getSnapshot().get98thPercentile());
		collector.record("query.queryExecution.99thpercentile", queryExecutionTimer().getSnapshot().get99thPercentile());
	}

}
