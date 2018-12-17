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

		  
		  File query_file = new File("../../query_2b.txt");
		  QueryParser q = new QueryParser(query_file);
		  if (q.R1_hf !=null || q.R2_hf != null) {
			  
		  FileScan am = null;
		  try {
		      am = new FileScan("R1.in",
					   q.R1types, null, (short) q.R1_no_flds, q.R1_no_flds,
					   q.R1_projection,null);
		    }
		    
		    catch (Exception e) {
		      System.err.println ("*** Error creating scan for Index scan");
		      System.err.println (""+e);
		      Runtime.getRuntime().exit(1);
		    }
		    
		    
		    NestedLoopsJoins nlj = null;
		    
			    try {
			      nlj = new NestedLoopsJoins (q.R1types,q.R1_no_flds, null,
							  q.R2types, q.R2_no_flds, null,
							  10,
							  am, (q.relations.size()>1)? "R2.in" : "R1.in",
							  q.Predicate, null, q.q_projection ,2);
			    }
			    
			    catch (Exception e) {
			      System.err.println ("*** Error preparing for nested_loop_join");
			      System.err.println (""+e);
			      e.printStackTrace();
			      Runtime.getRuntime().exit(1);
			    }
			    
			    Tuple t = new Tuple();
			    t = null;
			    try {
			      while ((t = nlj.get_next()) != null) {
			        t.print(q.projectionTypes);
			      }
			    }
			    catch (Exception e) {
			      System.err.println (""+e);
			      e.printStackTrace();
			      Runtime.getRuntime().exit(1);
			    }
		    }
		    else {
		    	
		    	if (q.R1_hf==null && q.R2_hf == null) {
		    		
		    		String relation = q.relations.get(0);
		    		
		    		File rel_file = new File("../../"+relation+".txt");
		    		
		    		try {
	  					// new relation file reader
	  					BufferedReader rel_reader1 = new BufferedReader(new FileReader(rel_file));
	  					BufferedReader rel_reader2 = new BufferedReader(new FileReader(rel_file));
	  					
	  					String rec1;
	  					String rec2;
	  					
	  					rec1 = rel_reader1.readLine();
	  					rec2 = rel_reader2.readLine();
	  					rel_reader1.mark(0);
	  					rel_reader2.mark(0);
	  					
	  					for(int i = 0; i<400 ;i++){
	  						
	  						
	  						int R_count;
		  					Tuple t = new Tuple();
				  		    try {
				  		      t.setHdr((short) q.R1_no_flds,q.R1types, null);
				  		    }
				  		    catch (Exception e) {
				  		      System.err.println("*** error in Tuple.setHdr() ***");
				  		      e.printStackTrace();
				  		    }
				  		    
				  		    int size = t.size();
				  		    
				  		    // inserting the tuple into the heap file "R1.in"
				  		    RID             rid;
				  		    try {
				  		    	q.R1_hf = new Heapfile("R1.in");
				  		    }
				  		    catch (Exception e) {
				  		      System.err.println("*** error in Heapfile constructor ***");
				  		      e.printStackTrace();
				  		    }
				  		    
				  		    t = new Tuple(size);
				  		    try {
				  		      t.setHdr((short) q.R1_no_flds, q.R1types, null);
				  		    }
				  		    catch (Exception e) {
				  		      System.err.println("*** error in Tuple.setHdr() ***");
				  		      e.printStackTrace();
				  		    }
				  		    
				  		      try {
				  			  		R_count = 0;
				  			  		rel_reader1.reset();
				  			  		while((rec1 = rel_reader1.readLine()) != null && R_count<5000) {
				  			  			
				  			  			// read each field for each tuple
				  			  			List<String> fields = Arrays.asList(rec1.split(","));		  		
				  			  			t.setIntFld(1, Integer.parseInt(fields.get(0)));
				  				  		t.setIntFld(2, Integer.parseInt(fields.get(1)));
				  				  		t.setIntFld(3, Integer.parseInt(fields.get(2)));
				  				  		t.setIntFld(4, Integer.parseInt(fields.get(3)));
				  				  		
				  				        try {
				  				        	// insert the tuple into the heap file
				  				        	rid = q.R1_hf.insertRecord(t.returnTupleByteArray());
				  				        	R_count++;
				  				              }
				  				              catch (Exception e) {
				  				        	System.err.println("*** error in Heapfile.insertRecord() ***");
				  				        	
				  				        	e.printStackTrace();
				  				              }  
				  			  		}
				  			  		
				  			  	rel_reader1.mark(0);
				  		  		
				  		      }
				  		      catch (Exception e) {
				  			System.err.println("*** Heapfile error in Tuple.setStrFld() ***");
				  			e.printStackTrace();
				  		      }
	  						
	  						for(int j = 0; j< 400; j++) {
	  							
				  					t = new Tuple();
						  		    try {
						  		      t.setHdr((short) q.R2_no_flds,q.R2types, null);
						  		    }
						  		    catch (Exception e) {
						  		      System.err.println("*** error in Tuple.setHdr() ***");
						  		      e.printStackTrace();
						  		    }
						  		    
						  		    size = t.size();
						  		    
						  		    // inserting the tuple into the heap file "R1.in"
						  		    
						  		    try {
						  		    	q.R2_hf = new Heapfile("R2.in");
						  		    }
						  		    catch (Exception e) {
						  		      System.err.println("*** error in Heapfile constructor ***");
						  		      e.printStackTrace();
						  		    }
						  		    
						  		    t = new Tuple(size);
						  		    try {
						  		      t.setHdr((short) q.R2_no_flds, q.R2types, null);
						  		    }
						  		    catch (Exception e) {
						  		      System.err.println("*** error in Tuple.setHdr() ***");
						  		      e.printStackTrace();
						  		    }
						  			
			
						  		      try {
						  			  		R_count = 0;
						  			  		rel_reader2.reset();
						  			  		while((rec2 = rel_reader2.readLine()) != null && R_count<5000) {
						  			  			
						  			  			// read each field for each tuple
						  			  			List<String> fields = Arrays.asList(rec2.split(","));		  		
						  			  			t.setIntFld(1, Integer.parseInt(fields.get(0)));
						  				  		t.setIntFld(2, Integer.parseInt(fields.get(1)));
						  				  		t.setIntFld(3, Integer.parseInt(fields.get(2)));
						  				  		t.setIntFld(4, Integer.parseInt(fields.get(3)));
						  				  		
						  				        try {
						  				        	// insert the tuple into the heap file
						  				        	rid = q.R2_hf.insertRecord(t.returnTupleByteArray());
						  				        	R_count++;
						  				              }
						  				              catch (Exception e) {
						  				        	System.err.println("*** error in Heapfile.insertRecord() ***");
						  				        	
						  				        	e.printStackTrace();
						  				              }  
						  			  		}
						  			  		
						  			  	rel_reader2.mark(0);
						  		  		
						  		      }
						  		      catch (Exception e) {
						  			System.err.println("*** Heapfile error in Tuple.setStrFld() ***");
						  			e.printStackTrace();
						  		      }
						  		      
						  		    FileScan am = null;
				  					  try {
				  					      am = new FileScan("R1.in",
				  								   q.R1types, null, (short) q.R1_no_flds, q.R1_no_flds,
				  								   q.R1_projection,null);
				  					    }
				  					    
				  					    catch (Exception e) {
				  					      System.err.println ("*** Error creating scan for Index scan");
				  					      System.err.println (""+e);
				  					      Runtime.getRuntime().exit(1);
				  					    }
				  					    
				  					    
				  					    NestedLoopsJoins nlj = null;
				  					    
				  						    try {
				  						      nlj = new NestedLoopsJoins (q.R1types,q.R1_no_flds, null,
				  										  q.R2types, q.R2_no_flds, null,
				  										  10,
				  										  am, (q.relations.size()>1)? "R2.in" : "R1.in",
				  										  q.Predicate, null, q.q_projection ,2);
				  						    }
				  						    
				  						    catch (Exception e) {
				  						      System.err.println ("*** Error preparing for nested_loop_join");
				  						      System.err.println (""+e);
				  						      e.printStackTrace();
				  						      Runtime.getRuntime().exit(1);
				  						    }
				  						    
				  						    t = new Tuple();
				  						    t = null;
				  						    try {
				  						      while ((t = nlj.get_next()) != null) {
				  						        t.print(q.projectionTypes);
				  						      }
				  						    }
				  						    catch (Exception e) {
				  						      System.err.println (""+e);
				  						      e.printStackTrace();
				  						      Runtime.getRuntime().exit(1);
				  						    }
				  						    

							  		    	q.R2_hf.deleteFile();
	  						}
	  					}

	  					
	  				}catch(Exception e) {
	  					System.err.println (""+e);
	  				}
		    		
		    	}
		    }
		    
		    
	  }
	  
	  public static void ParserTest2()
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

		  
		  File query_file = new File("../../query_2a.txt");
		  QueryParser q = new QueryParser(query_file);
		  if (q.R1_hf !=null || q.R2_hf != null) {
			  
		  FileScan am = null;
		  try {
		      am = new FileScan("R1.in",
					   q.R1types, null, (short) q.R1_no_flds, q.R1_no_flds,
					   q.R1_projection,null);
		    }
		    
		    catch (Exception e) {
		      System.err.println ("*** Error creating scan for Index scan");
		      System.err.println (""+e);
		      Runtime.getRuntime().exit(1);
		    }
		    
		    
		  SelfJoinOnePredicate nlj = null;
		    
			    try {
			      nlj = new SelfJoinOnePredicate (q.R1types,q.R1_no_flds, null,
							  q.R2types, q.R2_no_flds, null,
							  10,
							  am, (q.relations.size()>1)? "R2.in" : "R1.in",
							  q.Predicate, null, q.q_projection ,2);
			    }
			    
			    catch (Exception e) {
			      System.err.println ("*** Error preparing for nested_loop_join");
			      System.err.println (""+e);
			      e.printStackTrace();
			      Runtime.getRuntime().exit(1);
			    }
			    
			    Tuple t = new Tuple();
			    t = null;
			    try {
			      while ((t = nlj.get_next()) != null) {
			        t.print(q.projectionTypes);
			      }
			    }
			    catch (Exception e) {
			      System.err.println (""+e);
			      e.printStackTrace();
			      Runtime.getRuntime().exit(1);
			    }
		    }
		    else {
		    	
		    	if (q.R1_hf==null && q.R2_hf == null) {
		    		
		    		String relation = q.relations.get(0);
		    		
		    		File rel_file = new File("../../"+relation+".txt");
		    		
		    		try {
	  					// new relation file reader
	  					BufferedReader rel_reader1 = new BufferedReader(new FileReader(rel_file));
	  					BufferedReader rel_reader2 = new BufferedReader(new FileReader(rel_file));
	  					
	  					String rec1;
	  					String rec2;
	  					
	  					rec1 = rel_reader1.readLine();
	  					rec2 = rel_reader2.readLine();
	  					rel_reader1.mark(0);
	  					rel_reader2.mark(0);
	  					
	  					for(int i = 0; i<400 ;i++){
	  						
	  						
	  						int R_count;
		  					Tuple t = new Tuple();
				  		    try {
				  		      t.setHdr((short) q.R1_no_flds,q.R1types, null);
				  		    }
				  		    catch (Exception e) {
				  		      System.err.println("*** error in Tuple.setHdr() ***");
				  		      e.printStackTrace();
				  		    }
				  		    
				  		    int size = t.size();
				  		    
				  		    // inserting the tuple into the heap file "R1.in"
				  		    RID             rid;
				  		    try {
				  		    	q.R1_hf = new Heapfile("R1.in");
				  		    }
				  		    catch (Exception e) {
				  		      System.err.println("*** error in Heapfile constructor ***");
				  		      e.printStackTrace();
				  		    }
				  		    
				  		    t = new Tuple(size);
				  		    try {
				  		      t.setHdr((short) q.R1_no_flds, q.R1types, null);
				  		    }
				  		    catch (Exception e) {
				  		      System.err.println("*** error in Tuple.setHdr() ***");
				  		      e.printStackTrace();
				  		    }
				  		    
				  		      try {
				  			  		R_count = 0;
				  			  		rel_reader1.reset();
				  			  		while((rec1 = rel_reader1.readLine()) != null && R_count<5000) {
				  			  			
				  			  			// read each field for each tuple
				  			  			List<String> fields = Arrays.asList(rec1.split(","));		  		
				  			  			t.setIntFld(1, Integer.parseInt(fields.get(0)));
				  				  		t.setIntFld(2, Integer.parseInt(fields.get(1)));
				  				  		t.setIntFld(3, Integer.parseInt(fields.get(2)));
				  				  		t.setIntFld(4, Integer.parseInt(fields.get(3)));
				  				  		
				  				        try {
				  				        	// insert the tuple into the heap file
				  				        	rid = q.R1_hf.insertRecord(t.returnTupleByteArray());
				  				        	R_count++;
				  				              }
				  				              catch (Exception e) {
				  				        	System.err.println("*** error in Heapfile.insertRecord() ***");
				  				        	
				  				        	e.printStackTrace();
				  				              }  
				  			  		}
				  			  		
				  			  	rel_reader1.mark(0);
				  		  		
				  		      }
				  		      catch (Exception e) {
				  			System.err.println("*** Heapfile error in Tuple.setStrFld() ***");
				  			e.printStackTrace();
				  		      }
	  						
	  						for(int j = 0; j< 400; j++) {
	  							
				  					t = new Tuple();
						  		    try {
						  		      t.setHdr((short) q.R2_no_flds,q.R2types, null);
						  		    }
						  		    catch (Exception e) {
						  		      System.err.println("*** error in Tuple.setHdr() ***");
						  		      e.printStackTrace();
						  		    }
						  		    
						  		    size = t.size();
						  		    
						  		    // inserting the tuple into the heap file "R1.in"
						  		    
						  		    try {
						  		    	q.R2_hf = new Heapfile("R2.in");
						  		    }
						  		    catch (Exception e) {
						  		      System.err.println("*** error in Heapfile constructor ***");
						  		      e.printStackTrace();
						  		    }
						  		    
						  		    t = new Tuple(size);
						  		    try {
						  		      t.setHdr((short) q.R2_no_flds, q.R2types, null);
						  		    }
						  		    catch (Exception e) {
						  		      System.err.println("*** error in Tuple.setHdr() ***");
						  		      e.printStackTrace();
						  		    }
						  			
			
						  		      try {
						  			  		R_count = 0;
						  			  		rel_reader2.reset();
						  			  		while((rec2 = rel_reader2.readLine()) != null && R_count<5000) {
						  			  			
						  			  			// read each field for each tuple
						  			  			List<String> fields = Arrays.asList(rec2.split(","));		  		
						  			  			t.setIntFld(1, Integer.parseInt(fields.get(0)));
						  				  		t.setIntFld(2, Integer.parseInt(fields.get(1)));
						  				  		t.setIntFld(3, Integer.parseInt(fields.get(2)));
						  				  		t.setIntFld(4, Integer.parseInt(fields.get(3)));
						  				  		
						  				        try {
						  				        	// insert the tuple into the heap file
						  				        	rid = q.R2_hf.insertRecord(t.returnTupleByteArray());
						  				        	R_count++;
						  				              }
						  				              catch (Exception e) {
						  				        	System.err.println("*** error in Heapfile.insertRecord() ***");
						  				        	
						  				        	e.printStackTrace();
						  				              }  
						  			  		}
						  			  		
						  			  	rel_reader2.mark(0);
						  		  		
						  		      }
						  		      catch (Exception e) {
						  			System.err.println("*** Heapfile error in Tuple.setStrFld() ***");
						  			e.printStackTrace();
						  		      }
						  		      
						  		    FileScan am = null;
				  					  try {
				  					      am = new FileScan("R1.in",
				  								   q.R1types, null, (short) q.R1_no_flds, q.R1_no_flds,
				  								   q.R1_projection,null);
				  					    }
				  					    
				  					    catch (Exception e) {
				  					      System.err.println ("*** Error creating scan for Index scan");
				  					      System.err.println (""+e);
				  					      Runtime.getRuntime().exit(1);
				  					    }
				  					    
				  					    
				  					SelfJoinOnePredicate nlj = null;
				  					    
				  						    try {
				  						      nlj = new SelfJoinOnePredicate (q.R1types,q.R1_no_flds, null,
				  										  q.R2types, q.R2_no_flds, null,
				  										  10,
				  										  am, (q.relations.size()>1)? "R2.in" : "R1.in",
				  										  q.Predicate, null, q.q_projection ,2);
				  						    }
				  						    
				  						    catch (Exception e) {
				  						      System.err.println ("*** Error preparing for nested_loop_join");
				  						      System.err.println (""+e);
				  						      e.printStackTrace();
				  						      Runtime.getRuntime().exit(1);
				  						    }
				  						    
				  						    t = new Tuple();
				  						    t = null;
				  						    try {
				  						      while ((t = nlj.get_next()) != null) {
				  						        t.print(q.projectionTypes);
				  						      }
				  						    }
				  						    catch (Exception e) {
				  						      System.err.println (""+e);
				  						      e.printStackTrace();
				  						      Runtime.getRuntime().exit(1);
				  						    }
				  						    

							  		    	q.R2_hf.deleteFile();
	  						}
	  					}

	  					
	  				}catch(Exception e) {
	  					System.err.println (""+e);
	  				}
		    		
		    	}
		    }
		    
		
	  }
	  
	  public static void ParserTest3()
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
		  if (q.R1_hf !=null || q.R2_hf != null) {
			  
		  FileScan am = null;
		  try {
		      am = new FileScan("R1.in",
					   q.R1types, null, (short) q.R1_no_flds, q.R1_no_flds,
					   q.R1_projection,null);
		    }
		    
		    catch (Exception e) {
		      System.err.println ("*** Error creating scan for Index scan");
		      System.err.println (""+e);
		      Runtime.getRuntime().exit(1);
		    }
		    
		    
		  SelfInequalityJoinTwoPredicate nlj = null;
		    
			    try {
			      nlj = new SelfInequalityJoinTwoPredicate (q.R1types,q.R1_no_flds, null,
							  q.R2types, q.R2_no_flds, null,
							  10,
							  am, (q.relations.size()>1)? "R2.in" : "R1.in",
							  q.Predicate, null, q.q_projection ,2);
			    }
			    
			    catch (Exception e) {
			      System.err.println ("*** Error preparing for nested_loop_join");
			      System.err.println (""+e);
			      e.printStackTrace();
			      Runtime.getRuntime().exit(1);
			    }
			    
			    Tuple t = new Tuple();
			    t = null;
			    try {
			      while ((t = nlj.get_next()) != null) {
			        t.print(q.projectionTypes);
			      }
			    }
			    catch (Exception e) {
			      System.err.println (""+e);
			      e.printStackTrace();
			      Runtime.getRuntime().exit(1);
			    }
		    }
		    else {
		    	
		    	if (q.R1_hf==null && q.R2_hf == null) {
		    		
		    		String relation = q.relations.get(0);
		    		
		    		File rel_file = new File("../../"+relation+".txt");
		    		
		    		try {
	  					// new relation file reader
	  					BufferedReader rel_reader1 = new BufferedReader(new FileReader(rel_file));
	  					BufferedReader rel_reader2 = new BufferedReader(new FileReader(rel_file));
	  					
	  					String rec1;
	  					String rec2;
	  					
	  					rec1 = rel_reader1.readLine();
	  					rec2 = rel_reader2.readLine();
	  					rel_reader1.mark(0);
	  					rel_reader2.mark(0);
	  					
	  					for(int i = 0; i<400 ;i++){
	  						
	  						
	  						int R_count;
		  					Tuple t = new Tuple();
				  		    try {
				  		      t.setHdr((short) q.R1_no_flds,q.R1types, null);
				  		    }
				  		    catch (Exception e) {
				  		      System.err.println("*** error in Tuple.setHdr() ***");
				  		      e.printStackTrace();
				  		    }
				  		    
				  		    int size = t.size();
				  		    
				  		    // inserting the tuple into the heap file "R1.in"
				  		    RID             rid;
				  		    try {
				  		    	q.R1_hf = new Heapfile("R1.in");
				  		    }
				  		    catch (Exception e) {
				  		      System.err.println("*** error in Heapfile constructor ***");
				  		      e.printStackTrace();
				  		    }
				  		    
				  		    t = new Tuple(size);
				  		    try {
				  		      t.setHdr((short) q.R1_no_flds, q.R1types, null);
				  		    }
				  		    catch (Exception e) {
				  		      System.err.println("*** error in Tuple.setHdr() ***");
				  		      e.printStackTrace();
				  		    }
				  		    
				  		      try {
				  			  		R_count = 0;
				  			  		rel_reader1.reset();
				  			  		while((rec1 = rel_reader1.readLine()) != null && R_count<5000) {
				  			  			
				  			  			// read each field for each tuple
				  			  			List<String> fields = Arrays.asList(rec1.split(","));		  		
				  			  			t.setIntFld(1, Integer.parseInt(fields.get(0)));
				  				  		t.setIntFld(2, Integer.parseInt(fields.get(1)));
				  				  		t.setIntFld(3, Integer.parseInt(fields.get(2)));
				  				  		t.setIntFld(4, Integer.parseInt(fields.get(3)));
				  				  		
				  				        try {
				  				        	// insert the tuple into the heap file
				  				        	rid = q.R1_hf.insertRecord(t.returnTupleByteArray());
				  				        	R_count++;
				  				              }
				  				              catch (Exception e) {
				  				        	System.err.println("*** error in Heapfile.insertRecord() ***");
				  				        	
				  				        	e.printStackTrace();
				  				              }  
				  			  		}
				  			  		
				  			  	rel_reader1.mark(0);
				  		  		
				  		      }
				  		      catch (Exception e) {
				  			System.err.println("*** Heapfile error in Tuple.setStrFld() ***");
				  			e.printStackTrace();
				  		      }
	  						
	  						for(int j = 0; j< 400; j++) {
	  							
				  					t = new Tuple();
						  		    try {
						  		      t.setHdr((short) q.R2_no_flds,q.R2types, null);
						  		    }
						  		    catch (Exception e) {
						  		      System.err.println("*** error in Tuple.setHdr() ***");
						  		      e.printStackTrace();
						  		    }
						  		    
						  		    size = t.size();
						  		    
						  		    // inserting the tuple into the heap file "R1.in"
						  		    
						  		    try {
						  		    	q.R2_hf = new Heapfile("R2.in");
						  		    }
						  		    catch (Exception e) {
						  		      System.err.println("*** error in Heapfile constructor ***");
						  		      e.printStackTrace();
						  		    }
						  		    
						  		    t = new Tuple(size);
						  		    try {
						  		      t.setHdr((short) q.R2_no_flds, q.R2types, null);
						  		    }
						  		    catch (Exception e) {
						  		      System.err.println("*** error in Tuple.setHdr() ***");
						  		      e.printStackTrace();
						  		    }
						  			
			
						  		      try {
						  			  		R_count = 0;
						  			  		rel_reader2.reset();
						  			  		while((rec2 = rel_reader2.readLine()) != null && R_count<5000) {
						  			  			
						  			  			// read each field for each tuple
						  			  			List<String> fields = Arrays.asList(rec2.split(","));		  		
						  			  			t.setIntFld(1, Integer.parseInt(fields.get(0)));
						  				  		t.setIntFld(2, Integer.parseInt(fields.get(1)));
						  				  		t.setIntFld(3, Integer.parseInt(fields.get(2)));
						  				  		t.setIntFld(4, Integer.parseInt(fields.get(3)));
						  				  		
						  				        try {
						  				        	// insert the tuple into the heap file
						  				        	rid = q.R2_hf.insertRecord(t.returnTupleByteArray());
						  				        	R_count++;
						  				              }
						  				              catch (Exception e) {
						  				        	System.err.println("*** error in Heapfile.insertRecord() ***");
						  				        	
						  				        	e.printStackTrace();
						  				              }  
						  			  		}
						  			  		
						  			  	rel_reader2.mark(0);
						  		  		
						  		      }
						  		      catch (Exception e) {
						  			System.err.println("*** Heapfile error in Tuple.setStrFld() ***");
						  			e.printStackTrace();
						  		      }
						  		      
						  		    FileScan am = null;
				  					  try {
				  					      am = new FileScan("R1.in",
				  								   q.R1types, null, (short) q.R1_no_flds, q.R1_no_flds,
				  								   q.R1_projection,null);
				  					    }
				  					    
				  					    catch (Exception e) {
				  					      System.err.println ("*** Error creating scan for Index scan");
				  					      System.err.println (""+e);
				  					      Runtime.getRuntime().exit(1);
				  					    }
				  					    
				  					    
				  					SelfInequalityJoinTwoPredicate nlj = null;
				  					    
				  						    try {
				  						      nlj = new SelfInequalityJoinTwoPredicate (q.R1types,q.R1_no_flds, null,
				  										  q.R2types, q.R2_no_flds, null,
				  										  10,
				  										  am, (q.relations.size()>1)? "R2.in" : "R1.in",
				  										  q.Predicate, null, q.q_projection ,2);
				  						    }
				  						    
				  						    catch (Exception e) {
				  						      System.err.println ("*** Error preparing for nested_loop_join");
				  						      System.err.println (""+e);
				  						      e.printStackTrace();
				  						      Runtime.getRuntime().exit(1);
				  						    }
				  						    
				  						    t = new Tuple();
				  						    t = null;
				  						    try {
				  						      while ((t = nlj.get_next()) != null) {
				  						        t.print(q.projectionTypes);
				  						      }
				  						    }
				  						    catch (Exception e) {
				  						      System.err.println (""+e);
				  						      e.printStackTrace();
				  						      Runtime.getRuntime().exit(1);
				  						    }
				  						    

							  		    	q.R2_hf.deleteFile();
	  						}
	  					}

	  					
	  				}catch(Exception e) {
	  					System.err.println (""+e);
	  				}
		    		
		    	}
		    }
	  }	  
	  
	  
	  
	  
	  
	  
	  public static void main(String argv[])
	  {
		  //ParserTest test = new ParserTest();
		  ParserTest3();
	  }


}
