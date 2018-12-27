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
import java.io.*; 

public class heapfiletest implements GlobalConst {
	
	public static void main(String argv[])
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

	  
		File query_file = new File("../../query_2b.txt");
		QueryParser q = new QueryParser(query_file);
		
		int row_count = 0;
		
		FileScan am = null;
		try {
		      
			am = new FileScan("R.in",
					   q.R1types, null, (short) q.R1_no_flds, q.R1_no_flds,
					   q.R1_projection,null);
			
		    }
		    
		    catch (Exception e) {
		      System.err.println ("*** Error creating scan for Index scan");
		      System.err.println (""+e);
		      Runtime.getRuntime().exit(1);
		    }
		am.firstHeapFileCall = true;
		try {
			
		am.reader = new BufferedReader(new FileReader("../../Q2.txt"));
		
		while(true)
		{
			if(!am.getNextHeapFile(q.R2_no_flds, q.R2types,"R.in")) {
				try {
				      
					am = new FileScan("R.in",
							   q.R1types, null, (short) q.R1_no_flds, q.R1_no_flds,
							   q.R1_projection,null);
					
				    }
				    
				    catch (Exception e) {
				      System.err.println ("*** Error creating scan for Index scan");
				      System.err.println (""+e);
				      Runtime.getRuntime().exit(1);
				    }
				    
					    Tuple t = new Tuple();
					    t = null;
					    try {
					      while ((t = am.get_next()) != null) {
//					    	  t.print(q.R1types);
					    	  
					    	  row_count++;
					      }

					    }
					    catch (Exception e) {
					      System.err.println (""+e);
					      e.printStackTrace();
					      Runtime.getRuntime().exit(1);
					    }
					    
					    Heapfile F = new Heapfile("R.in");
					    F.deleteFile();
			}else {
				try {
				      
					am = new FileScan("R.in",
							   q.R1types, null, (short) q.R1_no_flds, q.R1_no_flds,
							   q.R1_projection,null);
					
				    }
				    
				    catch (Exception e) {
				      System.err.println ("*** Error creating scan for Index scan");
				      System.err.println (""+e);
				      Runtime.getRuntime().exit(1);
				    }
				    
					    Tuple t = new Tuple();
					    t = null;
					    try {
					      while ((t = am.get_next()) != null) {
//					    	  t.print(q.R1types);
					    	  
					    	  row_count++;
					      }

					    }
					    catch (Exception e) {
					      System.err.println (""+e);
					      e.printStackTrace();
					      Runtime.getRuntime().exit(1);
					    }
					    
					    Heapfile F = new Heapfile("R.in");
					    F.deleteFile();
					    break;
			}
		}
		}catch (Exception e) {
			System.err.println (""+e);
		      e.printStackTrace();
		}
		
		System.out.println(row_count);
		
		
		
	}

}
