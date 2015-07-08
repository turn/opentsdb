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

	public static Timer downSampleTimer() {
		return QUERY_METRICS_REGISTRY.timer("downSampleTimer");
	}

	public static Timer aggregationTimer() {
		return QUERY_METRICS_REGISTRY.timer("aggregationTimer");
	}

	public static Timer interpolationTimer() {
		return QUERY_METRICS_REGISTRY.timer("interpolationTimer");
	}

	public static Timer resultProcessing() {
		return QUERY_METRICS_REGISTRY.timer("interpolationTimer");
	}

	public static void collectStats(StatsCollector collector) {

		collector.record("query.queries.count", numQueries().getCount());
		collector.record("query.metrics.count", numMetrics().getCount());
		collector.record("query.expressions.count", numExpressions().getCount());

		collector.record("query.scan.output", numberOfScannedPointsCounter().getCount());

		collector.record("query.response.input", numberOfPointsInResponse().getCount());
		collector.record("query.response.serialized", numberOfResponsePointsSerialized().getCount());

		collector.record("query.processScan.mean", processScan().getSnapshot().getMean());
		collector.record("query.processScan.max", processScan().getSnapshot().getMax());
		collector.record("query.processScan.min", processScan().getSnapshot().getMin());
		collector.record("query.processScan.75thpercentile", processScan().getSnapshot().get75thPercentile());
		collector.record("query.processScan.95thpercentile", processScan().getSnapshot().get95thPercentile());
		collector.record("query.processScan.98thpercentile", processScan().getSnapshot().get98thPercentile());
		collector.record("query.processScan.99thpercentile", processScan().getSnapshot().get99thPercentile());

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

		collector.record("query.resultProcessing.max", resultProcessing().getSnapshot().getMax());
		collector.record("query.resultProcessing.min", resultProcessing().getSnapshot().getMin());
		collector.record("query.resultProcessing.mean", resultProcessing().getSnapshot().get75thPercentile());
		collector.record("query.resultProcessing.75thpercentile", resultProcessing().getSnapshot().get75thPercentile());
		collector.record("query.resultProcessing.95thpercentile", resultProcessing().getSnapshot().get98thPercentile());
		collector.record("query.resultProcessing.98thpercentile", resultProcessing().getSnapshot().get98thPercentile());
		collector.record("query.resultProcessing.99thpercentile", resultProcessing().getSnapshot().get99thPercentile());

	}
}
