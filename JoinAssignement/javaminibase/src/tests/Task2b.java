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

public class Task2b {

	public static void main(String argv[]) {
		long start = System.currentTimeMillis();
		ParserTestIEJoin<SelfInequalityJoinTwoPredicate> test = new ParserTestIEJoin<SelfInequalityJoinTwoPredicate>("../../query_2b.txt",
				"Output/Joined_Result_Query_2b.txt","SelfInequalityJoinTwoPredicate");
		long end = System.currentTimeMillis();
		System.out.println("SelfJoin two predicates Task2b takes " + (end - start) + "ms");

	}
}
