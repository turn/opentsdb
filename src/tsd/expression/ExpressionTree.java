package net.opentsdb.tsd.expression;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import java.util.List;
import java.util.Map;

public class ExpressionTree {

    private final Expression expr;

    private List<ExpressionTree> subExpressions;
    private List<String> funcParams;
    private Map<Integer, String> subMetricQueries;

    public ExpressionTree(Expression expr) {
        this.expr = expr;
    }

    public void addSubExpression(ExpressionTree child) {
        if (subExpressions == null) {
            subExpressions = Lists.newArrayList();
        }
        subExpressions.add(child);
    }

    public void addSubMetricQuery(String metricQuery, int magic) {
        if (subMetricQueries == null) {
            subMetricQueries = Maps.newHashMap();
        }
        subMetricQueries.put(magic, metricQuery);
    }

    public void addFunctionParameter(String param) {
        if (funcParams == null) {
            funcParams = Lists.newArrayList();
        }
        funcParams.add(param);
    }
}