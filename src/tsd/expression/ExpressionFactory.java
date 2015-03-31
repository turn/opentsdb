package net.opentsdb.tsd.expression;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Maps;
import net.opentsdb.core.DataPoints;
import net.opentsdb.core.Functions;
import net.opentsdb.core.TSQuery;

import java.util.List;
import java.util.Map;

public class ExpressionFactory {

  private static Map<String, Expression> availableFunctions =
          Maps.newHashMap();

  static {
    availableFunctions.put("id", new IdentityExpression());
    availableFunctions.put("alias", new AliasFunction());
    availableFunctions.put("scale", new Functions.ScaleFunction());
    availableFunctions.put("sumSeries", new Functions.SumSeriesFunction());
    availableFunctions.put("sum", new Functions.SumSeriesFunction());
    availableFunctions.put("difference", new Functions.DifferenceSeriesFunction());
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

    @Override
    public DataPoints[] evaluate(TSQuery data_query, List<DataPoints[]> queryResults,
                                 List<String> queryParams) {
      if (queryResults == null || queryResults.size() == 0) {
        throw new NullPointerException("No query results");
      }

      return queryResults.get(0);
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
