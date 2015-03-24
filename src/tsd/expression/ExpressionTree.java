package net.opentsdb.tsd.expression;

import com.google.common.base.Joiner;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import net.opentsdb.core.DataPoints;

import java.util.Collections;
import java.util.List;
import java.util.Map;

public class ExpressionTree {

  private final Expression expr;

  private List<ExpressionTree> subExpressions;
  private List<String> funcParams;
  private Map<Integer, String> subMetricQueries;
  private Map<Integer, Parameter> parameterSourceIndex = Maps.newHashMap();

  private static final Joiner DOUBLE_COMMA_JOINER = Joiner.on(",,").skipNulls();

  enum Parameter {
    SUB_EXPRESSION,
    METRIC_QUERY
  }

  public ExpressionTree(Expression expr) {
    this.expr = expr;
  }

  public void addSubExpression(ExpressionTree child, int paramIndex) {
    if (subExpressions == null) {
      subExpressions = Lists.newArrayList();
    }
    subExpressions.add(child);
    parameterSourceIndex.put(paramIndex, Parameter.SUB_EXPRESSION);
  }

  public void addSubMetricQuery(String metricQuery, int magic,
                                int paramIndex) {
    if (subMetricQueries == null) {
      subMetricQueries = Maps.newHashMap();
    }
    subMetricQueries.put(magic, metricQuery);
    parameterSourceIndex.put(paramIndex, Parameter.METRIC_QUERY);
  }

  public void addFunctionParameter(String param) {
    if (funcParams == null) {
      funcParams = Lists.newArrayList();
    }
    funcParams.add(param);
  }

  public DataPoints[] evaluate(List<DataPoints[]> queryResults) {
    List<DataPoints[]> materialized = Lists.newArrayList();
    List<Integer> metricQueryKeys = null;
    if (subMetricQueries != null && subMetricQueries.size() > 0) {
      metricQueryKeys = Lists.newArrayList(subMetricQueries.keySet());
      Collections.sort(metricQueryKeys);
    }

    int metricPointer = 0;
    int subExprPointer = 0;
    for (int i=0; i<parameterSourceIndex.size(); i++) {
      Parameter p = parameterSourceIndex.get(i);

      if (p == Parameter.METRIC_QUERY) {
        if (metricQueryKeys == null) {
          throw new RuntimeException("Attempt to read metric " +
                  "results when none exist");
        }

        int ix = metricQueryKeys.get(metricPointer++);
        materialized.add(queryResults.get(ix));
      }
      else if (p == Parameter.SUB_EXPRESSION) {
        ExpressionTree st = subExpressions.get(subExprPointer++);
        materialized.add(st.evaluate(queryResults));
      } else {
        throw new RuntimeException("Unknown value: " + p);
      }
    }

    return expr.evaluate(materialized);
  }

  public String toString() {
    return expr + "(" + DOUBLE_COMMA_JOINER.join(cleanMetricQueriesForDisplay(),
            createSubExpressionsString(), createFuncParamsForDisplay()) + ")";
  }

  private String createSubExpressionsString() {
    if (subExpressions == null) {
      return null;
    } else {
      return DOUBLE_COMMA_JOINER.join(subExpressions);
    }
  }

  private String createFuncParamsForDisplay() {
    if (funcParams == null) {
      return null;
    } else {
      return DOUBLE_COMMA_JOINER.join(funcParams);
    }
  }

  private String cleanMetricQueriesForDisplay() {
    List<String> metrics = Lists.newArrayList();
    if (subMetricQueries == null) {
      return null;
    }

    for (String m: subMetricQueries.values()) {
      metrics.add(m.replaceAll("\\{.*\\}", ""));
    }

    return DOUBLE_COMMA_JOINER.join(metrics);
  }
}