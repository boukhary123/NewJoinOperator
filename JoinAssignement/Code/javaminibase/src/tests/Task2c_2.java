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

public class Task2c_2 {

	public static void main(String argv[]) {
		long start = System.currentTimeMillis();
		ParserTestIEJoin<InequalityJoinTwoPredicates> test = new ParserTestIEJoin<InequalityJoinTwoPredicates>("../../../../Output/query_2c_2.txt",
				"../../../../Output/Joined_Result_Query_2c_2.txt","InequalityJoinTwoPredicates");
		long end = System.currentTimeMillis();
		System.out.println("IEJoin two predicates Task2c_2 takes " + (end - start) + "ms");

	}
}
