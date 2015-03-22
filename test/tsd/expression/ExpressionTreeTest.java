package net.opentsdb.tsd.expression;

import com.google.common.collect.Lists;
import net.opentsdb.core.DataPoints;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.List;

public class ExpressionTreeTest {

    @BeforeClass
    public static void setup() {
        ExpressionFactory.addFunction("foo", new FooExpression());
    }

    @Test
    public void parseSimple() {
        String expr = "foo(sum:proc.sys.cpu)";
        List<String> metricQueries = Lists.newArrayList();
        ExpressionTree tree = Expressions.parse(expr, metricQueries);
        System.out.println(metricQueries);
    }

    @Test
    public void parseMultiParameter() {
        String expr = "foo(sum:proc.sys.cpu,, sum:proc.meminfo.memfree)";
        List<String> metricQueries = Lists.newArrayList();
        ExpressionTree tree = Expressions.parse(expr, metricQueries);
        System.out.println(metricQueries);
    }

    @Test
    public void parseNestedExpr() {
        String expr = "foo(sum:proc.sys.cpu,, foo(sum:proc.a.b))";
        List<String> metricQueries = Lists.newArrayList();
        ExpressionTree tree = Expressions.parse(expr, metricQueries);
        System.out.println(metricQueries);
    }

    @Test
    public void parseExprWithParam() {
        String expr = "foo(sum:proc.sys.cpu,, 100,, 3.1415)";
        List<String> metricQueries = Lists.newArrayList();
        ExpressionTree tree = Expressions.parse(expr, metricQueries);
        System.out.println(metricQueries);
    }

    static class FooExpression implements Expression {
        @Override
        public DataPoints[] evaluate(List<DataPoints[]> queryResults) {
            return new DataPoints[0];
        }
    }
}
