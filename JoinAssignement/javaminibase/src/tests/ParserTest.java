package tests;

import iterator.*;
import heap.*;
import global.*;
import index.*;
import java.io.*;
import java.util.*;
import java.lang.*;
import diskmgr.*;
import bufmgr.*;
import btree.*; 
import catalog.*;
import tests.*;


public class ParserTest {
	
	  public static void main(String argv[])
	  {
		  File query_file = new File("../../query_2a.txt");
		  QueryParser q = new QueryParser(query_file);
	  }

}
