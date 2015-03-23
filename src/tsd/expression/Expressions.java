package net.opentsdb.tsd.expression;

import com.google.common.base.Preconditions;
import com.google.common.base.Splitter;

import java.util.List;

public class Expressions {

    private static final Splitter PARAM_SPLITTER =
            Splitter.on(",,").trimResults();

    public static ExpressionTree parse(String expr,
                                       List<String> metricQueries) {
        Preconditions.checkNotNull(expr);
        if (expr.indexOf('(') == -1 || expr.indexOf(')') == -1) {
            throw new RuntimeException("Invalid Expression: " + expr);
        }

        ExprReader reader = new ExprReader(expr.toCharArray());
        reader.skipWhitespaces();

        String funcName = reader.readFuncName();
        Expression rootExpr = ExpressionFactory.getByName(funcName);
        if (rootExpr == null) {
            throw new RuntimeException("Could not find evaluator " +
                    "for function '" + funcName + "'");
        }

        ExpressionTree root = new ExpressionTree(rootExpr);

        reader.skipWhitespaces();
        if (reader.peek() == '(') {
            reader.next();
            parse(reader, metricQueries, root);
        }

        return root;
    }

    private static void parse(ExprReader reader, List<String> metricQueries,
                              ExpressionTree root) {

        int parameterIndex = 0;
        reader.skipWhitespaces();
        if (reader.peek() != ')') {
            String param = reader.readNextParameter();
            parseParam(param, metricQueries, root, parameterIndex++);
        }

        while (true) {
            reader.skipWhitespaces();
            if (reader.peek() == ')') {
                return;
            } else if (reader.isNextSeq(",,")) {
                reader.skip(2); //swallow the ",," delimiter
                reader.skipWhitespaces();
                String param = reader.readNextParameter();
                parseParam(param, metricQueries, root, parameterIndex++);
            } else {
                throw new RuntimeException("Invalid delimiter in parameter " +
                        "list at pos=" + reader.getMark() + ", expr="
                        + reader.toString());
            }
        }
    }

    private static void parseParam(String param, List<String> metricQueries,
                                   ExpressionTree root, int index) {
        if (param == null || param.length() == 0) {
            throw new RuntimeException("Invalid Parameter in " +
                    "Expression");
        }

        if (param.indexOf('(') > 0 && param.indexOf(')') > 0) {
            // sub expression
            ExpressionTree subTree = parse(param, metricQueries);
            root.addSubExpression(subTree, index);
        } else if (param.indexOf(':') >= 0) {
            // metric query
            metricQueries.add(param);
            root.addSubMetricQuery(param, metricQueries.size() - 1, index);
        } else {
            // expression parameter
            root.addFunctionParameter(param);
        }
    }

}
