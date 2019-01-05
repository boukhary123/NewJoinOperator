package tests;

import iterator.*;
import heap.*;
import global.*;
import index.*;
import java.io.*;
import java.util.*;
import diskmgr.*;
import bufmgr.*;
import btree.*;
import catalog.*;
import tests.*;

public class Task2d_2c {

	public static void main(String argv[]) {
		long start = System.currentTimeMillis();
		ParserTestIEJoin<InequalityJoinTwoPredicatesOptimized> test = new ParserTestIEJoin<InequalityJoinTwoPredicatesOptimized>("../../../query_2c.txt",
				"../Output/Joined_Result_Query_2d_2c.txt","InequalityJoinTwoPredicatesOptimized");
		long end = System.currentTimeMillis();
		System.out.println("optimized IEJoin two predicates Task2d on query 2c takes " + (end - start) + "ms");

	}
}
