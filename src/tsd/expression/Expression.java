package net.opentsdb.tsd.expression;

import net.opentsdb.core.DataPoints;
import net.opentsdb.core.TSQuery;

import java.util.List;

public interface Expression {

  DataPoints[] evaluate(TSQuery data_query, List<DataPoints[]> queryResults, List<String> queryParams);

  String writeStringField(List<String> queryParams, String innerExpression);

}
