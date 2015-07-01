package net.opentsdb.tsd.expression;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Joiner;
import com.google.common.collect.Maps;
import net.opentsdb.core.DataPoint;
import net.opentsdb.core.DataPoints;
import net.opentsdb.core.Functions;
import net.opentsdb.core.MutableDataPoint;
import net.opentsdb.core.PostAggregatedDataPoints;
import net.opentsdb.core.TSQuery;
import org.apache.log4j.Logger;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class ExpressionFactory {

  private static final Logger logger = Logger.getLogger(ExpressionFactory.class);

  private static Map<String, Expression> availableFunctions =
          Maps.newHashMap();

  static {
    availableFunctions.put("id", new IdentityExpression());
    availableFunctions.put("alias", new AliasFunction());
    availableFunctions.put("scale", new Functions.ScaleFunction());
    availableFunctions.put("sumSeries", new Functions.SumSeriesFunction());
    availableFunctions.put("sum", new Functions.SumSeriesFunction());
    availableFunctions.put("difference", new Functions.DifferenceSeriesFunction());
    availableFunctions.put("multiply", new Functions.MultiplySeriesFunction());
    availableFunctions.put("divide", new Functions.DivideSeriesFunction());
    availableFunctions.put("movingAverage", new Functions.MovingAverageFunction());
    availableFunctions.put("highestCurrent", new Functions.HighestCurrent());
    availableFunctions.put("highestMax", new Functions.HighestMax());
  }

  @VisibleForTesting
  static void addFunction(String name, Expression expr) {
    availableFunctions.put(name, expr);
  }

  public static Expression getByName(String funcName) {
    return availableFunctions.get(funcName);
  }

  static class IdentityExpression implements Expression {
    @Override
    public DataPoints[] evaluate(TSQuery data_query,
                                 List<DataPoints[]> queryResults, List<String> queryParams) {
      return queryResults.get(0);
    }

    @Override
    public String toString() {
      return "id";
    }

    @Override
    public String writeStringField(List<String> queryParams, String innerExpression) {
      return "id(" + innerExpression + ")";
    }
  }

  static class AliasFunction implements Expression {

    static Joiner COMMA_JOINER = Joiner.on(',').skipNulls();

    @Override
    public DataPoints[] evaluate(TSQuery data_query, List<DataPoints[]> queryResults,
                                 List<String> queryParams) {
      if (queryResults == null || queryResults.size() == 0) {
        throw new NullPointerException("No query results");
      }

      String aliasTemplate = "__default";

      if (queryParams != null && queryParams.size() >= 0) {
        aliasTemplate = COMMA_JOINER.join(queryParams);
      }

      DataPoints[] inputPoints = queryResults.get(0);

      DataPoint[][] dps = new DataPoint[inputPoints.length][];

      for (int j=0; j<dps.length; j++) {
        DataPoints base = inputPoints[j];
        dps[j] = new DataPoint[base.size()];
        int i = 0;

        for (DataPoint pt : base) {
          if (pt.isInteger()) {
            dps[j][i] = MutableDataPoint.ofDoubleValue(pt.timestamp(), pt.longValue());
          } else {
            dps[j][i] = MutableDataPoint.ofDoubleValue(pt.timestamp(), pt.doubleValue());
          }
          i++;
        }
      }

      logger.info(", queryResults.size=" + queryResults.size()
              + ", queryResults(0).length=" + inputPoints.length);

      DataPoints[] resultArray = new DataPoints[queryResults.get(0).length];
      for (int i=0; i<resultArray.length; i++) {
        PostAggregatedDataPoints result = new PostAggregatedDataPoints(inputPoints[i],
                dps[i]);

        String alias = aliasTemplate;
        for (Map.Entry<String, String> e: inputPoints[i].getTags().entrySet()) {
          alias = alias.replace("@" + e.getKey(), e.getValue());
        }

        result.setAlias(alias);
        resultArray[i] = result;
      }

      return resultArray;
    }

    @Override
    public String writeStringField(List<String> queryParams, String innerExpression) {
      if (queryParams == null || queryParams.size() == 0) {
        return "NULL";
      }

      return queryParams.get(0);
    }
  }
}
