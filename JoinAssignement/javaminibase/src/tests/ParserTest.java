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

// this class is used to get the result of the query and to compare the results with
// with nlj to test our implementation

class Row_to_compare{
	
	public int fld1,fld2; // first and second field of the returned fields of the query
	public Row_to_compare(int fld1,int fld2){
		this.fld1=fld1;
		this.fld2=fld2;
	}
}


class Sortasceding implements Comparator<Row_to_compare> 
{ 
	// Used for sorting in ascending order of 
	// fld1
	public int compare(Row_to_compare a, Row_to_compare b) 
	{ 
		int comp = a.fld1 - b.fld1;
		
		if(comp==0)
			return a.fld2-b.fld2;;
		
		return comp;
			
	} 
} 


public class ParserTest  implements GlobalConst {
	
	public static ArrayList<Row_to_compare> L_nlj;
	public static ArrayList<Row_to_compare> L_ieqjoin;
	public static ArrayList<Row_to_compare> L_selfjoin_one;
	public static ArrayList<Row_to_compare> L_selfjoin_two;


	
	  public ParserTest(String path)
	  { 
		  L_nlj = new ArrayList<Row_to_compare>();
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

		  
		  File query_file = new File(path);
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
			        L_nlj.add(new Row_to_compare(t.getIntFld(1),t.getIntFld(2)));
			      }
			      Collections.sort(L_nlj,new Sortasceding());
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
		  L_selfjoin_one = new ArrayList<Row_to_compare>();
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
			        L_selfjoin_one.add(new Row_to_compare(t.getIntFld(1),t.getIntFld(2)));
			      }
			      Collections.sort(L_selfjoin_one, new Sortasceding());
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
		  L_selfjoin_two = new ArrayList<Row_to_compare>();
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
				        L_selfjoin_two.add(new Row_to_compare(t.getIntFld(1), t.getIntFld(2)));
				      }
				      Collections.sort(L_selfjoin_two, new Sortasceding());
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
	  
	  
	  
	  public static void ParserTest4()
	  {
		  L_ieqjoin = new ArrayList<Row_to_compare>();
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

		  
		  File query_file = new File("../../query_2c_1.txt");
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
		    
		    
		  InequalityJoinTwoPredicates nlj = null;
		    
//			    try {
//			      nlj = new SelfInequalityJoinTwoPredicate (q.R1types,q.R1_no_flds, null,
//							  q.R2types, q.R2_no_flds, null,
//							  10,
//							  am, (q.relations.size()>1)? "R2.in" : "R1.in",
//							  q.Predicate, null, q.q_projection ,2);
//			    }
			    
			    
			    
			    try {
				      nlj = new InequalityJoinTwoPredicates (q.R1types,q.R1_no_flds, null,
								  q.R2types, q.R2_no_flds, null,
								  10,
								  am, "R1.in","R2.in",
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
				        L_ieqjoin.add(new Row_to_compare(t.getIntFld(1),t.getIntFld(2)));
				      }
				      Collections.sort(L_ieqjoin, new Sortasceding());
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
				  					    
				  					    
				  					InequalityJoinTwoPredicates nlj = null;
				  					    
//				  						    try {
//				  						      nlj = new SelfInequalityJoinTwoPredicate (q.R1types,q.R1_no_flds, null,
//				  										  q.R2types, q.R2_no_flds, null,
//				  										  10,
//				  										  am, (q.relations.size()>1)? "R2.in" : "R1.in",
//				  										  q.Predicate, null, q.q_projection ,2);
//				  						    }
				  					
				  					
				  				    try {
				  				      nlj = new InequalityJoinTwoPredicates (q.R1types,q.R1_no_flds, null,
				  								  q.R2types, q.R2_no_flds, null,
				  								  10,
				  								  am, "R1.in","R2.in",
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
	  
	  public static void compare_2_arrays(ArrayList<Row_to_compare> L1, ArrayList<Row_to_compare> L2) {
		  
		  
		  int i=0;
		  boolean results_same = true;
		  if (L1.size()==L2.size()) {
			  System.out.println("Both arrays are of same size");
		  
		  
		  for (i=0;i<L1.size();i++) {
			  if(L2.get(i).fld1==L2.get(i).fld1 &&
					  L1.get(i).fld2==L2.get(i).fld2){
				  results_same = true;
						  }
			  else {
				  results_same = false;
				  break;
				  }
			  }
		  }
		  else {
			  System.out.println("Results have different size");
			  Runtime.getRuntime().exit(1);

		  }

		  if (results_same) {
			  System.out.println("Identical Results");
		  }
		  else
			  System.out.println("Results are different"); 
			  
	  }
		  
	  
	  	  
	  public static void main(String argv[])
	  {
		  long start = System.currentTimeMillis();
		  ParserTest test = new ParserTest("../../query_2a.txt");
	      long end = System.currentTimeMillis(); 
		  
	      System.out.println("NLJ takes " + 
                  (end - start) + "ms"); 
	      start = System.currentTimeMillis();
		  ParserTest2();
		  end = System.currentTimeMillis(); 
		  System.out.println("IESelfJoin takes " + 
                  (end - start) + "ms");	
		  
		  compare_2_arrays(L_nlj,L_selfjoin_one);
		  }
}
