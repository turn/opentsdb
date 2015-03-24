package net.opentsdb.tsd.expression;

import net.opentsdb.core.DataPoints;

import java.util.List;

public interface Expression {

  DataPoints[] evaluate(List<DataPoints[]> queryResults, List<String> queryParams);

  String writeStringField(List<String> queryParams, String innerExpression);

}
