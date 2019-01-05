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

public class Task2a {

	public static void main(String argv[]) {
		long start = System.currentTimeMillis();
		ParserTestIEJoin<SelfJoinOnePredicate> test = new ParserTestIEJoin<SelfJoinOnePredicate>("../../../query_2a.txt",
				"../Output/Joined_Result_Query_2a.txt","SelfJoinOnePredicate");
		long end = System.currentTimeMillis();
		System.out.println("SelfJoin One predicate Task2a takes " + (end - start) + "ms");

	}
}
