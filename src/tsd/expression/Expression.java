package net.opentsdb.tsd.expression;

import net.opentsdb.core.DataPoints;

import java.util.List;

public interface Expression {

    DataPoints[] evaluate(List<DataPoints[]> queryResults);

}
