/**
 * Copyright (C) 2015 Turn Inc. All Rights Reserved.
 * Proprietary and confidential.
 */
package net.opentsdb.tsd.expression;

import java.util.List;
import java.util.Map;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.stumbleupon.async.Deferred;
import net.opentsdb.core.AggregationIterator;
import net.opentsdb.core.Aggregators;
import net.opentsdb.core.DataPoint;
import net.opentsdb.core.DataPoints;
import net.opentsdb.core.MutableDataPoint;
import net.opentsdb.core.PostAggregatedDataPoints;
import net.opentsdb.core.SeekableView;
import net.opentsdb.meta.Annotation;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class InterpolationTest {

  private static final Logger LOG =
          LoggerFactory.getLogger(AggregationIterator.class);

  @Test
  public void simpleTest() {
    int count = 6;
    DataPoint[] points1 = new DataPoint[count];
    points1[0] = MutableDataPoint.ofDoubleValue(0, 5);
    points1[1] = MutableDataPoint.ofDoubleValue(10, 5);
    points1[2] = MutableDataPoint.ofDoubleValue(20, 10);
    points1[3] = MutableDataPoint.ofDoubleValue(30, 15);
    points1[4] = MutableDataPoint.ofDoubleValue(40, 20);
    points1[5] = MutableDataPoint.ofDoubleValue(50, 5);

    DataPoint[] points2 = new DataPoint[count];
    points2[0] = MutableDataPoint.ofDoubleValue(0, 10);
    points2[1] = MutableDataPoint.ofDoubleValue(10, 5);
    points2[2] = MutableDataPoint.ofDoubleValue(20, 20);
    points2[3] = MutableDataPoint.ofDoubleValue(30, 15);
    points2[4] = MutableDataPoint.ofDoubleValue(40, 10);
    points2[5] = MutableDataPoint.ofDoubleValue(50, 0);

    PostAggregatedDataPoints dps1 = new PostAggregatedDataPoints(new MockDataPoints(), points1);
    PostAggregatedDataPoints dps2 = new PostAggregatedDataPoints(new MockDataPoints(), points2);

    AggregationIterator aggr = new AggregationIterator(new SeekableView[]
            {dps1.iterator(), dps2.iterator()}, 0, 100,
            Aggregators.SUM, Aggregators.Interpolation.LERP, false);

    while (aggr.hasNext()) {
      DataPoint dp = aggr.next();
      LOG.info(dp.timestamp() + " " + (dp.isInteger()? dp.longValue() : dp.doubleValue()));
    }
  }

  @Test
  public void interpolateTest() {
    DataPoint[] points1 = new DataPoint[] {
            MutableDataPoint.ofDoubleValue(10, 5),
            MutableDataPoint.ofDoubleValue(30, 15),
            MutableDataPoint.ofDoubleValue(50, 5)
    };

    DataPoint[] points2 = new DataPoint[] {
            MutableDataPoint.ofDoubleValue(0, 10),
            MutableDataPoint.ofDoubleValue(20, 20),
            MutableDataPoint.ofDoubleValue(40, 10),
            MutableDataPoint.ofDoubleValue(60, 20)
    };

    PostAggregatedDataPoints dps1 = new PostAggregatedDataPoints(new MockDataPoints(), points1);
    PostAggregatedDataPoints dps2 = new PostAggregatedDataPoints(new MockDataPoints(), points2);

    AggregationIterator aggr = new AggregationIterator(new SeekableView[]
            {dps1.iterator(), dps2.iterator()}, 0, 100,
            Aggregators.SUM, Aggregators.Interpolation.LERP, false);

    while (aggr.hasNext()) {
      DataPoint dp = aggr.next();
      LOG.info(dp.timestamp() + " " + (dp.isInteger()? dp.longValue() : dp.doubleValue()));
    }
  }

  static class MockDataPoints implements DataPoints {

    @Override
    public String metricName() {
      return "testMetric";
    }

    @Override
    public Deferred<String> metricNameAsync() {
      return Deferred.fromResult(metricName());
    }

    @Override
    public Map<String, String> getTags() {
      Map<String, String> p = Maps.newHashMap();
      p.put("tagk", "tagv");
      return p;
    }

    @Override
    public Deferred<Map<String, String>> getTagsAsync() {
      return Deferred.fromResult(getTags());
    }

    @Override
    public List<String> getAggregatedTags() {
      return Lists.newArrayList("type");
    }

    @Override
    public Deferred<List<String>> getAggregatedTagsAsync() {
      return Deferred.fromResult(getAggregatedTags());
    }

    @Override
    public List<String> getTSUIDs() {
      return null;
    }

    @Override
    public List<Annotation> getAnnotations() {
      return null;
    }

    @Override
    public int size() {
      throw new RuntimeException("not implemented");
    }

    @Override
    public int aggregatedSize() {
      throw new RuntimeException("not implemented");
    }

    @Override
    public SeekableView iterator() {
      throw new RuntimeException("not implemented");
    }

    @Override
    public long timestamp(int i) {
      throw new RuntimeException("not implemented");
    }

    @Override
    public boolean isInteger(int i) {
      throw new RuntimeException("not implemented");
    }

    @Override
    public long longValue(int i) {
      throw new RuntimeException("not implemented");
    }

    @Override
    public double doubleValue(int i) {
      throw new RuntimeException("not implemented");
    }
  }

}
