/**
 * Copyright (C) 2015 Turn Inc. All Rights Reserved.
 * Proprietary and confidential.
 */
package net.opentsdb.core;

import java.util.List;
import java.util.Map;

import com.stumbleupon.async.Deferred;
import net.opentsdb.meta.Annotation;

public class PostAggregatedDataPoints implements DataPoints {

  private final DataPoints baseDataPoints;
  private final DataPoint[] points;

  public PostAggregatedDataPoints(DataPoints baseDataPoints, DataPoint[] points) {
    this.baseDataPoints = baseDataPoints;
    this.points = points;
  }

  @Override
  public String metricName() {
    return baseDataPoints.metricName();
  }

  @Override
  public Deferred<String> metricNameAsync() {
    return baseDataPoints.metricNameAsync();
  }

  @Override
  public Map<String, String> getTags() {
    return baseDataPoints.getTags();
  }

  @Override
  public Deferred<Map<String, String>> getTagsAsync() {
    return baseDataPoints.getTagsAsync();
  }

  @Override
  public List<String> getAggregatedTags() {
    return baseDataPoints.getAggregatedTags();
  }

  @Override
  public Deferred<List<String>> getAggregatedTagsAsync() {
    return baseDataPoints.getAggregatedTagsAsync();
  }

  @Override
  public List<String> getTSUIDs() {
    return baseDataPoints.getTSUIDs();
  }

  @Override
  public List<Annotation> getAnnotations() {
    return baseDataPoints.getAnnotations();
  }

  @Override
  public int size() {
    return points.length;
  }

  @Override
  public int aggregatedSize() {
    return points.length;
  }

  @Override
  public SeekableView iterator() {
    return new SeekableViewImpl(points);
  }

  @Override
  public long timestamp(int i) {
    return points[i].timestamp();
  }

  @Override
  public boolean isInteger(int i) {
    return points[i].isInteger();
  }

  @Override
  public long longValue(int i) {
    return points[i].longValue();
  }

  @Override
  public double doubleValue(int i) {
    return points[i].doubleValue();
  }

  static class SeekableViewImpl implements SeekableView {

    private int pos=0;
    private final DataPoint[] dps;

    public SeekableViewImpl(DataPoint[] dps) {
      this.dps = dps;
    }

    @Override
    public boolean hasNext() {
      return pos < dps.length;
    }

    @Override
    public DataPoint next() {
      return dps[pos++];
    }

    @Override
    public void remove() {
      throw new RuntimeException("Not supported exception");
    }

    @Override
    public void seek(long timestamp) {
      for (int i=pos; i<dps.length; i++) {
        if (dps[i].timestamp() >= timestamp) {
          break;
        } else {
          pos++;
        }
      }
    }
  }

}
