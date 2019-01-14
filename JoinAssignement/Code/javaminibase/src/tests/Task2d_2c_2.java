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

public class Task2d_2c_2 {

	public static void main(String argv[]) {
		long start = System.currentTimeMillis();
		ParserTestIEJoin<InequalityJoinTwoPredicatesOptimized> test = new ParserTestIEJoin<InequalityJoinTwoPredicatesOptimized>("../../../../Output/query_2c_2.txt",
				"../../../../Output/Joined_Result_Query_2c_2.txt","InequalityJoinTwoPredicatesOptimized");
		long end = System.currentTimeMillis();
		System.out.println("Optimized IEJoin two predicates Task2d on Task2c_2 takes " + (end - start) + "ms");

	}
}
