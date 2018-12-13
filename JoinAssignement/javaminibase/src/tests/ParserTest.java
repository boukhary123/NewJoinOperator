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


public class ParserTest  implements GlobalConst {
	
	  public ParserTest()
	  {
		  String dbpath = "/tmp/"+System.getProperty("user.name")+".minibase.jointestdb"; 
		    String logpath = "/tmp/"+System.getProperty("user.name")+".joinlog";

		    String remove_cmd = "/bin/rm -rf ";
		    String remove_logcmd = remove_cmd + logpath;
		    String remove_dbcmd = remove_cmd + dbpath;
		    String remove_joincmd = remove_cmd + dbpath;

		    try {
		      Runtime.getRuntime().exec(remove_logcmd);
		      Runtime.getRuntime().exec(remove_dbcmd);
		      Runtime.getRuntime().exec(remove_joincmd);
		    }
		    catch (IOException e) {
		      System.err.println (""+e);
		    }

		   
		    /*
		    ExtendedSystemDefs extSysDef = 
		      new ExtendedSystemDefs( "/tmp/minibase.jointestdb", "/tmp/joinlog",
					      1000,500,200,"Clock");
		    */

		    SystemDefs sysdef = new SystemDefs( dbpath, 1000, NUMBUF, "Clock" );

		  
		  File query_file = new File("../../query_1a.txt");
		  QueryParser q = new QueryParser(query_file);
	  }
	  
	  public static void main(String argv[])
	  {
		  ParserTest test = new ParserTest();

	  }


}
