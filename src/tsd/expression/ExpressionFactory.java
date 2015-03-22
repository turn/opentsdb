package net.opentsdb.tsd.expression;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Maps;

import java.util.Map;

public class ExpressionFactory {

    private static Map<String, Expression> availableFunctions =
            Maps.newHashMap();

    @VisibleForTesting
    static void addFunction(String name, Expression expr) {
        availableFunctions.put(name, expr);
    }

    public static Expression getByName(String funcName) {
        return availableFunctions.get(funcName);
    }

}
