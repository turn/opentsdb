/**
 * Copyright (C) 2015 Turn Inc. All Rights Reserved.
 * Proprietary and confidential.
 */
package net.opentsdb.core;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import com.google.common.collect.Lists;
import com.google.common.math.DoubleMath;
import net.opentsdb.tsd.expression.Expression;
import net.opentsdb.tsd.expression.ExpressionTree;
import org.apache.log4j.Logger;
import org.slf4j.LoggerFactory;

public class Functions {

  private static final org.slf4j.Logger LOG =
          LoggerFactory.getLogger(Functions.class);

  public static class MovingAverageFunction implements Expression {

    @Override
    public DataPoints[] evaluate(TSQuery data_query, List<DataPoints[]> queryResults, List<String> params) {
      if (queryResults == null || queryResults.isEmpty()) {
        return new DataPoints[]{};
      }

      if (params == null || params.isEmpty()) {
        throw new NullPointerException("Need aggregation window for moving average");
      }
      String param = params.get(0);
      if (param == null || param.length() == 0) {
        throw new NullPointerException("Invalid window='" + param + "'");
      }

      param = param.trim();

      long numPoints = -1;
      boolean isTimeUnit = false;
      if (param.matches("[0-9]+")) {
        numPoints = Integer.parseInt(param);
      } else if (param.startsWith("'") && param.endsWith("'")) {
        numPoints = parseParam(param);
        isTimeUnit = true;
      }

      if (numPoints <= 0) {
        throw new RuntimeException("numPoints <= 0");
      }

      int size = 0;
      for (DataPoints[] results: queryResults) {
        size = size + results.length;
      }

      PostAggregatedDataPoints[] seekablePoints = new PostAggregatedDataPoints[size];
      int ix=0;
      for (DataPoints[] results: queryResults) {
        for (DataPoints dpoints: results) {
          List<DataPoint> mutablePoints = new ArrayList<DataPoint>();
          for (DataPoint point: dpoints) {
            mutablePoints.add(point.isInteger() ?
                    MutableDataPoint.ofLongValue(point.timestamp(), point.longValue())
                    : MutableDataPoint.ofDoubleValue(point.timestamp(), point.doubleValue()));
          }
          seekablePoints[ix++] = new PostAggregatedDataPoints(dpoints,
                  mutablePoints.toArray(new DataPoint[mutablePoints.size()]));
        }
      }

      SeekableView[] views = new SeekableView[size];
      for (int i=0; i<size; i++) {
        views[i] = seekablePoints[i].iterator();
      }

      SeekableView view = new AggregationIterator(views,
              data_query.startTime(), data_query.endTime(),
              new Aggregators.MovingAverage(Aggregators.Interpolation.LERP, "movingAverage", numPoints, isTimeUnit),
              Aggregators.Interpolation.LERP, false);

      List<DataPoint> points = Lists.newArrayList();
      while (view.hasNext()) {
        DataPoint mdp = view.next();
        points.add(mdp.isInteger() ?
                MutableDataPoint.ofLongValue(mdp.timestamp(), mdp.longValue()) :
                MutableDataPoint.ofDoubleValue(mdp.timestamp(), mdp.doubleValue()));
      }

      if (queryResults.size() > 0 && queryResults.get(0).length > 0) {
        return new DataPoints[]{new PostAggregatedDataPoints(queryResults.get(0)[0],
                points.toArray(new DataPoint[points.size()]))};
      } else {
        return new DataPoints[]{};
      }
    }

    public long parseParam(String param) {
      char[] chars = param.toCharArray();
      int tuIndex = 0;
      for (int c = 1; c < chars.length; c++) {
        if (Character.isDigit(chars[c])) {
          tuIndex++;
        } else {
          break;
        }
      }

      if (tuIndex == 0) {
        throw new RuntimeException("Invalid Parameter: " + param);
      }

      int time = Integer.parseInt(param.substring(1, tuIndex+1));
      String unit = param.substring(tuIndex+1, param.length() - 1);

      if ("min".equals(unit)) {
        return TimeUnit.MILLISECONDS.convert(time, TimeUnit.MINUTES);
      } else if ("hr".equals(unit)) {
        return TimeUnit.MILLISECONDS.convert(time, TimeUnit.HOURS);
      } else if ("sec".equals(unit)) {
        return TimeUnit.MILLISECONDS.convert(time, TimeUnit.SECONDS);
      } else {
        throw new RuntimeException("unknown time unit=" + unit);
      }

    }

    @Override
    public String writeStringField(List<String> queryParams, String innerExpression) {
      return "movingAverage(" + innerExpression + ")";
    }

  }

  public static class HighestCurrent {

  }

  public static class HighestMax {

  }

  public static class DivideSeriesFunction implements Expression {

    @Override
    public DataPoints[] evaluate(TSQuery data_query, List<DataPoints[]> queryResults, List<String> params) {
      if (queryResults == null || queryResults.isEmpty()) {
        throw new NullPointerException("Query results cannot be empty");
      }

      DataPoints x,y;
      if (queryResults.size() == 2 && queryResults.get(0).length == 1
              && queryResults.get(1).length == 1) {
        x = queryResults.get(0)[0];
        y = queryResults.get(1)[0];
      } else if (queryResults.size() == 1 && queryResults.get(0).length == 2) {
        x = queryResults.get(0)[0];
        y = queryResults.get(0)[1];
      } else {
        throw new RuntimeException("Expected two query results for difference");
      }

      int size = 2;
      PostAggregatedDataPoints[] seekablePoints = new PostAggregatedDataPoints[size];

      List<DataPoint> mutablePoints = new ArrayList<DataPoint>();
      for (DataPoint point: x) {
        mutablePoints.add(point.isInteger() ?
                MutableDataPoint.ofLongValue(point.timestamp(), point.longValue())
                : MutableDataPoint.ofDoubleValue(point.timestamp(), point.doubleValue()));
      }
      seekablePoints[0] = new PostAggregatedDataPoints(x,
              mutablePoints.toArray(new DataPoint[mutablePoints.size()]));

      mutablePoints = new ArrayList<DataPoint>();
      for (DataPoint point: y) {
        if (point.isInteger()) {
          if (point.longValue() != 0) {
            mutablePoints.add(MutableDataPoint.ofLongValue(point.timestamp(),
                    1 / point.longValue()));
          }
        } else {
          if (DoubleMath.fuzzyCompare(point.doubleValue(), 0, 1E-7) != 0) {
            mutablePoints.add(MutableDataPoint.ofDoubleValue(point.timestamp(),
                    1 / point.doubleValue()));
          }
        }
      }

      seekablePoints[1] = new PostAggregatedDataPoints(x,
              mutablePoints.toArray(new DataPoint[mutablePoints.size()]));

      SeekableView[] views = new SeekableView[size];
      for (int i=0; i<size; i++) {
        views[i] = seekablePoints[i].iterator();
      }

      SeekableView view = (new AggregationIterator(views,
              data_query.startTime(), data_query.endTime(),
              Aggregators.MULTIPLY, Aggregators.Interpolation.LERP, false));

      List<DataPoint> points = Lists.newArrayList();
      while (view.hasNext()) {
        DataPoint mdp = view.next();
        points.add(mdp.isInteger() ?
                MutableDataPoint.ofLongValue(mdp.timestamp(), mdp.longValue()) :
                MutableDataPoint.ofDoubleValue(mdp.timestamp(), mdp.doubleValue()));
      }

      if (queryResults.size() > 0 && queryResults.get(0).length > 0) {
        return new DataPoints[]{new PostAggregatedDataPoints(queryResults.get(0)[0],
                points.toArray(new DataPoint[points.size()]))};
      } else {
        return new DataPoints[]{};
      }
    }


    @Override
    public String writeStringField(List<String> queryParams, String innerExpression) {
      return "divideSeries(" + innerExpression + ")";
    }
  }

  public static class MultiplySeriesFunction implements Expression {

    @Override
    public DataPoints[] evaluate(TSQuery data_query, List<DataPoints[]> queryResults, List<String> queryParams) {
      if (queryResults == null || queryResults.isEmpty()) {
        throw new NullPointerException("Query results cannot be empty");
      }

      int size = 0;
      for (DataPoints[] results: queryResults) {
        size = size + results.length;
      }

      PostAggregatedDataPoints[] seekablePoints = new PostAggregatedDataPoints[size];
      int ix=0;
      for (DataPoints[] results: queryResults) {
        for (DataPoints dpoints: results) {
          List<DataPoint> mutablePoints = new ArrayList<DataPoint>();
          for (DataPoint point: dpoints) {
            mutablePoints.add(point.isInteger() ?
                    MutableDataPoint.ofLongValue(point.timestamp(), point.longValue())
                    : MutableDataPoint.ofDoubleValue(point.timestamp(), point.doubleValue()));
          }
          seekablePoints[ix++] = new PostAggregatedDataPoints(dpoints,
                  mutablePoints.toArray(new DataPoint[mutablePoints.size()]));
        }
      }

      SeekableView[] views = new SeekableView[size];
      for (int i=0; i<size; i++) {
        views[i] = seekablePoints[i].iterator();
      }

      SeekableView view = (new AggregationIterator(views,
              data_query.startTime(), data_query.endTime(),
              Aggregators.MULTIPLY, Aggregators.Interpolation.LERP, false));

      List<DataPoint> points = Lists.newArrayList();
      while (view.hasNext()) {
        DataPoint mdp = view.next();
        points.add(mdp.isInteger() ?
                MutableDataPoint.ofLongValue(mdp.timestamp(), mdp.longValue()) :
                MutableDataPoint.ofDoubleValue(mdp.timestamp(), mdp.doubleValue()));
      }

      if (queryResults.size() > 0 && queryResults.get(0).length > 0) {
        return new DataPoints[]{new PostAggregatedDataPoints(queryResults.get(0)[0],
                points.toArray(new DataPoint[points.size()]))};
      } else {
        return new DataPoints[]{};
      }
    }

    @Override
    public String writeStringField(List<String> queryParams, String innerExpression) {
      return "multiplySeries(" + innerExpression + ")";
    }
  }

  public static class DifferenceSeriesFunction implements Expression {

    @Override
    public DataPoints[] evaluate(TSQuery data_query, List<DataPoints[]> queryResults, List<String> params) {
      if (queryResults == null || queryResults.isEmpty()) {
        throw new NullPointerException("Query results cannot be empty");
      }

      DataPoints x,y;
      if (queryResults.size() == 2 && queryResults.get(0).length == 1
              && queryResults.get(1).length == 1) {
        x = queryResults.get(0)[0];
        y = queryResults.get(1)[0];
      } else if (queryResults.size() == 1 && queryResults.get(0).length == 2) {
        x = queryResults.get(0)[0];
        y = queryResults.get(0)[1];
      } else {
        throw new RuntimeException("Expected two query results for difference");
      }

      int size = 2;
      PostAggregatedDataPoints[] seekablePoints = new PostAggregatedDataPoints[size];

      List<DataPoint> mutablePoints = new ArrayList<DataPoint>();
      for (DataPoint point: x) {
        mutablePoints.add(point.isInteger() ?
                MutableDataPoint.ofLongValue(point.timestamp(), point.longValue())
                : MutableDataPoint.ofDoubleValue(point.timestamp(), point.doubleValue()));
      }
      seekablePoints[0] = new PostAggregatedDataPoints(x,
              mutablePoints.toArray(new DataPoint[mutablePoints.size()]));

      mutablePoints = new ArrayList<DataPoint>();
      for (DataPoint point: y) {
        mutablePoints.add(point.isInteger() ?
                MutableDataPoint.ofLongValue(point.timestamp(),
                        -1 * point.longValue())
                : MutableDataPoint.ofDoubleValue(point.timestamp(),
                -1 * point.doubleValue()));
      }
      seekablePoints[1] = new PostAggregatedDataPoints(x,
              mutablePoints.toArray(new DataPoint[mutablePoints.size()]));

      SeekableView[] views = new SeekableView[size];
      for (int i=0; i<size; i++) {
        views[i] = seekablePoints[i].iterator();
      }

      SeekableView view = (new AggregationIterator(views,
              data_query.startTime(), data_query.endTime(),
              Aggregators.SUM, Aggregators.Interpolation.LERP, false));

      List<DataPoint> points = Lists.newArrayList();
      while (view.hasNext()) {
        DataPoint mdp = view.next();
        points.add(mdp.isInteger() ?
                MutableDataPoint.ofLongValue(mdp.timestamp(), mdp.longValue()) :
                MutableDataPoint.ofDoubleValue(mdp.timestamp(), mdp.doubleValue()));
      }

      if (queryResults.size() > 0 && queryResults.get(0).length > 0) {
        return new DataPoints[]{new PostAggregatedDataPoints(queryResults.get(0)[0],
                points.toArray(new DataPoint[points.size()]))};
      } else {
        return new DataPoints[]{};
      }
    }

    @Override
    public String writeStringField(List<String> queryParams, String innerExpression) {
      return "differenceSeries(" + innerExpression + ")";
    }
  }

  public static class SumSeriesFunction implements Expression {

    @Override
    public DataPoints[] evaluate(TSQuery data_query, List<DataPoints[]> queryResults, List<String> params) {
      if (queryResults == null || queryResults.isEmpty()) {
        throw new NullPointerException("Query results cannot be empty");
      }

      int size = 0;
      for (DataPoints[] results: queryResults) {
        size = size + results.length;
      }

      PostAggregatedDataPoints[] seekablePoints = new PostAggregatedDataPoints[size];
      int ix=0;
      for (DataPoints[] results: queryResults) {
        for (DataPoints dpoints: results) {
          List<DataPoint> mutablePoints = new ArrayList<DataPoint>();
          for (DataPoint point: dpoints) {
            mutablePoints.add(point.isInteger() ?
                    MutableDataPoint.ofLongValue(point.timestamp(), point.longValue())
                    : MutableDataPoint.ofDoubleValue(point.timestamp(), point.doubleValue()));
//            LOG.info("Read> " + point.timestamp() + " " + point.toDouble());
          }
          seekablePoints[ix++] = new PostAggregatedDataPoints(dpoints,
                  mutablePoints.toArray(new DataPoint[mutablePoints.size()]));
//          LOG.info("-------------------------------");
        }
      }

//      LOG.info("start_time=" + data_query.startTime() + ", end_time=" + data_query.endTime());

      SeekableView[] views = new SeekableView[size];
      for (int i=0; i<size; i++) {
        views[i] = seekablePoints[i].iterator();
      }

      SeekableView view = (new AggregationIterator(views,
              data_query.startTime(), data_query.endTime(),
              Aggregators.SUM, Aggregators.Interpolation.LERP, false));

      List<DataPoint> points = Lists.newArrayList();
      while (view.hasNext()) {
        DataPoint mdp = view.next();
        points.add(mdp.isInteger() ?
                MutableDataPoint.ofLongValue(mdp.timestamp(), mdp.longValue()) :
                MutableDataPoint.ofDoubleValue(mdp.timestamp(), mdp.doubleValue()));
      }

      if (queryResults.size() > 0 && queryResults.get(0).length > 0) {
        return new DataPoints[]{new PostAggregatedDataPoints(queryResults.get(0)[0],
                points.toArray(new DataPoint[points.size()]))};
      } else {
        return new DataPoints[]{};
      }
    }

    @Override
    public String writeStringField(List<String> queryParams, String innerExpression) {
      return "sumSeries(" + innerExpression + ")";
    }
  }

  public static class ScaleFunction implements Expression {

    @Override
    public DataPoints[] evaluate(TSQuery data_query, List<DataPoints[]> queryResults, List<String> params) {
      if (queryResults == null || queryResults.isEmpty()) {
        throw new NullPointerException("Query results cannot be empty");
      }

      if (params == null || params.isEmpty()) {
        throw new NullPointerException("Scaling parameter not available");
      }

      String factor = params.get(0);
      factor = factor.replaceAll("'|\"", "").trim();
      double scaleFactor = Double.parseDouble(factor);

      DataPoints[] inputPoints = queryResults.get(0);
      DataPoints[] outputPoints = new DataPoints[inputPoints.length];

      for (int i=0; i<inputPoints.length; i++) {
        outputPoints[i] = scale(inputPoints[i], scaleFactor);
      }

      return outputPoints;
    }

    protected DataPoints scale(DataPoints points, double scaleFactor) {
      int size = points.size();
      DataPoint[] dps = new DataPoint[size];

      SeekableView view = points.iterator();
      int i=0;
      while (view.hasNext()) {
        DataPoint pt = view.next();
        if (pt.isInteger()) {
          dps[i] = MutableDataPoint.ofDoubleValue(pt.timestamp(), scaleFactor * pt.longValue());
        } else {
          dps[i] = MutableDataPoint.ofDoubleValue(pt.timestamp(), scaleFactor * pt.doubleValue());
        }
        i++;
      }

      return new PostAggregatedDataPoints(points, dps);
    }

    @Override
    public String writeStringField(List<String> queryParams, String innerExpression) {
      return "scale(" + innerExpression + ")";
    }
  }

}
