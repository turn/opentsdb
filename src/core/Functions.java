/**
 * Copyright (C) 2015 Turn Inc. All Rights Reserved.
 * Proprietary and confidential.
 */
package net.opentsdb.core;

import java.util.ArrayList;
import java.util.List;

import com.google.common.collect.Lists;
import net.opentsdb.tsd.expression.Expression;
import org.apache.log4j.Logger;
import org.slf4j.LoggerFactory;

public class Functions {

  private static final org.slf4j.Logger LOG =
          LoggerFactory.getLogger(Functions.class);


  public static class SumSeriesFunction implements Expression {

    @Override
    public DataPoints[] evaluate(TSQuery data_query, List<DataPoints[]> queryResults, List<String> params) {
      if (queryResults == null || queryResults.size() == 0) {
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
            LOG.info("Read> " + point.timestamp() + " " + point.toDouble());
          }
          seekablePoints[ix++] = new PostAggregatedDataPoints(dpoints,
                  mutablePoints.toArray(new DataPoint[mutablePoints.size()]));
          LOG.info("-------------------------------");
        }
      }

      LOG.info("start_time=" + data_query.startTime() + ", end_time=" + data_query.endTime());

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

//      SeekableView[] iterators = new SeekableView[size];
//      int ix=0;
//      for (DataPoints[] results: queryResults) {
//        for (DataPoints dpoints: results) {
//          iterators[ix] = dpoints.iterator();
//          iterators[ix].seek(data_query.startTime());
//          DataPoint dp = iterators[ix].next();
//          LOG.info(">> " + dp.timestamp() + " " + dp.toDouble());
//          ix++;
//        }
//      }
//
//      for (DataPoints[] results: queryResults) {
//        for (DataPoints dpoints: results) {
//          for (DataPoint dp : dpoints) {
//            LOG.info(">>" + dp.timestamp() + " " + (dp.isInteger() ? dp.longValue() : dp.doubleValue()));
//          }
//          LOG.info("Processed one line");
//        }
//      }
//
//      LOG.info("start_time=" + data_query.startTime() + ", end_time=" + data_query.endTime());
//
//      SeekableView view = (new AggregationIterator(iterators,
//              data_query.startTime(), data_query.endTime(),
//              Aggregators.SUM, Aggregators.Interpolation.LERP, false));
//
//      List<DataPoint> points = Lists.newArrayList();
//      while (view.hasNext()) {
//        points.add(view.next());
//      }
//
//      return new DataPoints[] {new PostAggregatedDataPoints(queryResults.get(0)[0],
//              points.toArray(new DataPoint[points.size()]))};
    }

    @Override
    public String writeStringField(List<String> queryParams, String innerExpression) {
      return "sumSeries(" + innerExpression + ")";
    }
  }

  public static class ScaleFunction implements Expression {

    @Override
    public DataPoints[] evaluate(TSQuery data_query, List<DataPoints[]> queryResults, List<String> params) {
      if (queryResults == null || queryResults.size() == 0) {
        throw new NullPointerException("Query results cannot be empty");
      }

      if (params == null || params.size() == 0) {
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
