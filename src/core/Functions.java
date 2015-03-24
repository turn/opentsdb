/**
 * Copyright (C) 2015 Turn Inc. All Rights Reserved.
 * Proprietary and confidential.
 */
package net.opentsdb.core;

import java.util.List;

import net.opentsdb.tsd.expression.Expression;

public class Functions {

  public static class ScaleFunction implements Expression {

    @Override
    public DataPoints[] evaluate(List<DataPoints[]> queryResults, List<String> params) {
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
