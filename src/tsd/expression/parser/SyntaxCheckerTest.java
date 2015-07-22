/**
 * Copyright (C) 2015 Turn Inc. All Rights Reserved.
 * Proprietary and confidential.
 */
package net.opentsdb.tsd.expression.parser;

import java.io.StringReader;
import java.util.ArrayList;
import java.util.List;

import net.opentsdb.core.TSQuery;
import net.opentsdb.tsd.expression.ExpressionTree;

public class SyntaxCheckerTest {

	public static void main(String[] args) {
		try {
			StringReader r = new StringReader("sum(sum:proc.stat.cpu.percpu{cpu=1})");
			SyntaxChecker checker = new SyntaxChecker(r);
			TSQuery query = new TSQuery();
			List<String> metrics = new ArrayList<String>();
			checker.setTSQuery(query);
			checker.setMetricQueries(metrics);
			ExpressionTree tree = checker.EXPRESSION();
			System.out.println("Syntax is okay. ExprTree=" + tree);
			System.out.println("Metrics=" + metrics);
		} catch (Throwable e) {
			System.out.println("Syntax check failed: " + e);
		}
	}
}
