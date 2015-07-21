/**
 * Copyright (C) 2015 Turn Inc. All Rights Reserved.
 * Proprietary and confidential.
 */
package net.opentsdb.tsd.expression.parser;

import java.util.ArrayList;
import java.util.List;

import net.opentsdb.core.TSQuery;
import net.opentsdb.tsd.expression.ExpressionTree;

public class Tester {

	public static void main(String[] args) {
		try {
			String expr = "alias(sum:1m-avg:CapEnforcementControl.DailyBudgetSpend.value{allocType=manual,serverid=control1,capType=Total,currency=USD},1)";
			SyntaxChecker checker = new SyntaxChecker(new java.io.StringReader(expr));
			List<String> metrics = new ArrayList<String>();
			checker.setMetricQueries(metrics);
			checker.setTSQuery(new TSQuery());

			ExpressionTree tree = checker.EXPRESSION();

			System.out.println("Syntax is okay, " + tree.toString());
			System.out.println("Metrics=" + metrics);
		} catch (Throwable e) {
			// Catching Throwable is ugly but JavaCC throws Error objects!
			System.out.println("Syntax check failed: ");
			e.printStackTrace();
		}
	}
}
