package net.opentsdb.tsd.expression;

import net.opentsdb.core.DataPoints;

import java.util.List;

public abstract class Expression {

    public abstract DataPoints[] evaluate(List<DataPoints[]> queryResults);

}
