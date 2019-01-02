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

public class Task2d_2b {

	public static void main(String argv[]) {
		long start = System.currentTimeMillis();
		ParserTestIEJoin<SelfInequalityJoinTwoPredicateOptimized> test = new ParserTestIEJoin<SelfInequalityJoinTwoPredicateOptimized>("../../query_2b.txt",
				"Output/Joined_Result_Query_2d_2b.txt","SelfInequalityJoinTwoPredicateOptimized");
		long end = System.currentTimeMillis();
		System.out.println("optimized SelfJoin two predicates Task 2d on query Task2b takes " + (end - start) + "ms");

	}
}
