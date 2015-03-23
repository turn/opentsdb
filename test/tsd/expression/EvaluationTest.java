package net.opentsdb.tsd.expression;

import com.google.common.collect.Lists;
import net.opentsdb.core.DataPoints;
import org.junit.Test;

import java.util.List;

public class EvaluationTest {

    @Test
    public void parseNestedExpr() {
        String expr = "id(sum:proc.meminfo.buffers,, id(sum:proc.meminfo.buffers))";
        List<String> metricQueries = Lists.newArrayList();
        ExpressionTree tree = Expressions.parse(expr, metricQueries);
        System.out.println(metricQueries);

        tree.evaluate(Lists.<DataPoints[]>newArrayList(null, null));
    }

}
