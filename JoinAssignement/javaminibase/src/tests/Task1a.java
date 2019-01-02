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

public class Task1a {

	public static void main(String argv[]) {
		long start = System.currentTimeMillis();
		ParserTestNlj test = new ParserTestNlj("../../query_1a.txt","Output/Joined_Result_Query_1a.txt");
		long end = System.currentTimeMillis();
		System.out.println("NLJ Task1a takes " + (end - start) + "ms");

	}
}
