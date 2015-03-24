package net.opentsdb.tsd.expression;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Maps;
import net.opentsdb.core.DataPoints;

import java.util.List;
import java.util.Map;

public class ExpressionFactory {

  private static Map<String, Expression> availableFunctions =
          Maps.newHashMap();

  static {
    availableFunctions.put("id", new IdentityExpression());
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
    public DataPoints[] evaluate(List<DataPoints[]> queryResults) {
      return queryResults.get(0);
    }

    @Override
    public String toString() {
      return "id";
    }

  }

}
