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

public class QueryParser {
	
	// list of relation names
	List<String> relations;
	
	// projection fields specifications for the query
	FldSpec []  q_projection; 
	
	// projection represeting all the fields specification of first relation (used in FileScan)
	FldSpec []  R1_projection; 
	
	// projection represeting all the fields specification of second relation (used in FileScan if needed)
	FldSpec []  R2_projection; 
	
	// Projection attribute type (used in print tuples)
	AttrType[] projectionTypes;
	
	// attribute types for the first relation
	AttrType[] R1types;
	
	// attribute types for the second relation
	AttrType[] R2types;
	
	// number of field for first relation
	int R1_no_flds;
	
	// number of field for first relation
	int R2_no_flds;
	
	// true if the query has a single predicate (condition)
	boolean singlePred;
	
	// condition expression of the first predicate
	CondExpr[] firstPred;
	
	// condition expression of the second predicate
	CondExpr[] secondPred;
	
	// heapfile for first relation
	Heapfile R1_hf;
	
	// heapfile for second relation
	Heapfile R2_hf;
	

	
	public QueryParser(File f)
	{
		// intialize as single predicate query
		singlePred = true;
		
		try {
		    	// initial the query file reader
				BufferedReader query_reader = new BufferedReader(new FileReader(f)); 
		  		
		  		String line;
		  		// read the first line of the query
		  		if((line = query_reader.readLine()) != null)
		  		{
		  			// put the fields in the projection in a list
		  			List<String> Select = Arrays.asList(line.split(" "));
		  			
		  			// initialize the projection field specification
		  			q_projection = new FldSpec[2];
		  			
		  			// initialize an array of types of the projected fields in the query
		  			projectionTypes = new AttrType[2];
		  			
		  			// get first field
	  				String field = Select.get(0);
	  				int index_init = field.indexOf('_');
	  				
	  				//extract the offset
	  				field = field.substring(index_init+1);
	  				
	  				// field specification object for the first projected field
	  				q_projection[0] = new FldSpec(new RelSpec(RelSpec.outer), Integer.parseInt(field));
	  				
		  			// get first field
	  				field = Select.get(1);
	  				index_init = field.indexOf('_');
	  				
	  				//extract the offset
	  				field = field.substring(index_init+1);
	  				
	  				// field specification object for the second projected field
	  				q_projection[1] = new FldSpec(new RelSpec(RelSpec.innerRel), Integer.parseInt(field));
	  				
	  				// read the relations involved (FROM)
	  				if((line = query_reader.readLine()) != null)
	  				{
	  					// extract the relation names
	  					relations = Arrays.asList(line.split(" "));
			  			
	  					// number of relations
	  					int no_relations = relations.size();
	  					
	  					// get first relation
		  				String relation = relations.get(0);
		  				
		  				// get the relation file containing the attribute types and records
		  				File rel_file = new File("../../"+relation+".txt");
		  				
		  				try {
		  					// new relation file reader
		  					BufferedReader rel_reader = new BufferedReader(new FileReader(rel_file));
		  					String header;
		  					// extract the header (contains the attribute types)
		  					if((header = rel_reader.readLine()) != null) {
		  						// extract the attribute types
					  			List<String> attributes = Arrays.asList(header.split(","));
					  			
					  			// number of fields
					  			R1_no_flds = attributes.size();
					  			
					  			// field specification for all the attributes
					  			R1_projection = new FldSpec[R1_no_flds];
					  			
					  			// atriute types
					  			R1types = new AttrType[R1_no_flds];
					  			
					  			for(int i = 0; i< R1_no_flds;i++) {
					  				
					  				// specify the type and specification for each attribute in the first relation
					  				switch(attributes.get(i)) {
					  				case "attrString":
					  					R1types[i] = new AttrType(0);
					  					R1_projection[i] = new FldSpec(new RelSpec(RelSpec.outer),i+1);
					  					break;
					  				case "attrInteger":
					  					R1types[i] = new AttrType(1);
					  					R1_projection[i] = new FldSpec(new RelSpec(RelSpec.outer),i+1);
					  					break;
					  				case "attrReal":
					  					R1types[i] = new AttrType(2);
					  					R1_projection[i] = new FldSpec(new RelSpec(RelSpec.outer),i+1);
					  					break;
					  				case "attrSymbol":
					  					R1types[i] = new AttrType(3);
					  					R1_projection[i] = new FldSpec(new RelSpec(RelSpec.outer),i+1);
					  					break;
					  				case "attrNull":
					  					R1types[i] = new AttrType(4);
					  					R1_projection[i] = new FldSpec(new RelSpec(RelSpec.outer),i+1);
					  					break;
					  				default:
					  					R1types[i] = new AttrType(4);
					  					R1_projection[i] = new FldSpec(new RelSpec(RelSpec.outer),i+1);
					  				}
					  			}
			  					
					  		    Tuple t = new Tuple();
					  		    try {
					  		      t.setHdr((short) R1_no_flds,R1types, null);
					  		    }
					  		    catch (Exception e) {
					  		      System.err.println("*** error in Tuple.setHdr() ***");
					  		      e.printStackTrace();
					  		    }
					  		    
					  		    int size = t.size();
					  		    
					  		    // inserting the tuple into the heap file "R1.in"
					  		    RID             rid;
					  		    R1_hf = null;
					  		    try {
					  		    	R1_hf = new Heapfile("R1.in");
					  		    }
					  		    catch (Exception e) {
					  		      System.err.println("*** error in Heapfile constructor ***");
					  		      e.printStackTrace();
					  		    }
					  		    
					  		    t = new Tuple(size);
					  		    try {
					  		      t.setHdr((short) R1_no_flds, R1types, null);
					  		    }
					  		    catch (Exception e) {
					  		      System.err.println("*** error in Tuple.setHdr() ***");
					  		      e.printStackTrace();
					  		    }
					  			

					  		      try {
						  		    	rel_reader = new BufferedReader(new FileReader(rel_file)); 

					  			  		String rec;
					  			  		rec = rel_reader.readLine();
					  			  		while((rec = rel_reader.readLine()) != null) {
					  			  			
					  			  			// read each field for each tuple
					  			  			List<String> fields = Arrays.asList(rec.split(","));		  		
					  			  			t.setIntFld(1, Integer.parseInt(fields.get(0)));
					  				  		t.setIntFld(2, Integer.parseInt(fields.get(1)));
					  				  		t.setIntFld(3, Integer.parseInt(fields.get(2)));
					  				  		t.setIntFld(4, Integer.parseInt(fields.get(3)));
					  				  		
					  				        try {
					  				        	// insert the tuple into the heap file
					  				        	rid = R1_hf.insertRecord(t.returnTupleByteArray());
					  				              }
					  				              catch (Exception e) {
					  				        	System.err.println("*** error in Heapfile.insertRecord() ***");
					  				        	
					  				        	e.printStackTrace();
					  				              }  
					  			  		}
					  		  		
					  		      }
					  		      catch (Exception e) {
					  			System.err.println("*** Heapfile error in Tuple.setStrFld() ***");
					  			e.printStackTrace();
					  		      }
	
		  					}
		  					
		  				}catch(Exception e) {
		  					
		  				}
		  				
		  				// if we have two relations
		  				if (no_relations > 1)
		  				{
		  					
		  					relation = relations.get(1);
			  				
		  					// read the relation file
			  				rel_file = new File("../../"+relation+".txt");
			  				
			  				try {
			  					// initialize reader
			  					BufferedReader rel_reader = new BufferedReader(new FileReader(rel_file));
			  					
			  					String header;
			  					
			  					// extract header
			  					if((header = rel_reader.readLine()) != null) {
			  						// extract attributes
						  			List<String> attributes = Arrays.asList(header.split(","));
						  			
						  			// number of attributes
						  			R2_no_flds = attributes.size();
						  			
						  			// attribute specification
						  			R2_projection = new FldSpec[R2_no_flds];

						  			// attribute types
						  			R2types = new AttrType[R2_no_flds];
						  			
						  			for(int i = 0; i< R2_no_flds;i++) {
						  				
						  				// specify the type and specification for each attribute
						  				switch(attributes.get(i)) {
						  				case "attrString":
						  					R2types[i] = new AttrType(0);
						  					R2_projection[i] = new FldSpec(new RelSpec(RelSpec.innerRel),i+1);
						  					break;
						  				case "attrInteger":
						  					R2types[i] = new AttrType(1);
						  					R2_projection[i] = new FldSpec(new RelSpec(RelSpec.innerRel),i+1);

						  					break;
						  				case "attrReal":
						  					R2types[i] = new AttrType(2);
						  					R2_projection[i] = new FldSpec(new RelSpec(RelSpec.innerRel),i+1);

						  					break;
						  				case "attrSymbol":
						  					R2types[i] = new AttrType(3);
						  					R2_projection[i] = new FldSpec(new RelSpec(RelSpec.innerRel),i+1);

						  					break;
						  				case "attrNull":
						  					R2types[i] = new AttrType(4);
						  					R2_projection[i] = new FldSpec(new RelSpec(RelSpec.innerRel),i+1);

						  					break;
						  				default:
						  					R2types[i] = new AttrType(4);
						  					R1_projection[i] = new FldSpec(new RelSpec(RelSpec.innerRel),i+1);

						  				}
						  			}
						  			
						  			// specify the type of each projected attribute in the query (types in SELECTion)
				  					projectionTypes[0] = new AttrType(R1types[q_projection[0].offset].attrType);
				  					projectionTypes[1] = new AttrType(R2types[q_projection[1].offset].attrType);
		
			  					}
			  					
			  					// create a tuple for heap file
			  					Tuple t = new Tuple();
					  		    try {
					  		      t.setHdr((short) R2_no_flds,R2types, null);
					  		    }
					  		    catch (Exception e) {
					  		      System.err.println("*** error in Tuple.setHdr() ***");
					  		      e.printStackTrace();
					  		    }
					  		    
					  		    int size = t.size();
					  		    
					  		    // inserting the tuple into file "sailors"
					  		    RID             rid;
					  		    R2_hf = null;
					  		    try {
					  		    	// create heap file for R2
					  		    	R2_hf = new Heapfile("R2.in");
					  		    }
					  		    catch (Exception e) {
					  		      System.err.println("*** error in Heapfile constructor ***");
					  		      e.printStackTrace();
					  		    }
					  		    
					  		    t = new Tuple(size);
					  		    try {
					  		      t.setHdr((short) R2_no_flds, R2types, null);
					  		    }
					  		    catch (Exception e) {
					  		      System.err.println("*** error in Tuple.setHdr() ***");
					  		      e.printStackTrace();
					  		    }
					  			

					  		      try {
					  		    	  
//					  			t.setIntFld(1, ((Sailor)sailors.elementAt(i)).sid);
//					  			t.setStrFld(2, ((Sailor)sailors.elementAt(i)).sname);
//					  			t.setIntFld(3, ((Sailor)sailors.elementAt(i)).rating);
//					  			t.setFloFld(4, (float)((Sailor)sailors.elementAt(i)).age);
					  					
					  		    	rel_reader = new BufferedReader(new FileReader(rel_file)); 
					  			  		
					  			  		String rec;
					  			  		rec = rel_reader.readLine();
					  			  		while((rec = rel_reader.readLine()) != null) {
					  			  			List<String> fields = Arrays.asList(rec.split(","));
					  			  			
					  			  			// extract attribute values for each record
					  			  			t.setIntFld(1, Integer.parseInt(fields.get(0)));
					  				  		t.setIntFld(2, Integer.parseInt(fields.get(1)));
					  				  		t.setIntFld(3, Integer.parseInt(fields.get(2)));
					  				  		t.setIntFld(4, Integer.parseInt(fields.get(3)));
					  				  		
					  				        try {
					  				        	// insert the tupe into the heap file
					  				        	rid = R2_hf.insertRecord(t.returnTupleByteArray());
					  				              }
					  				              catch (Exception e) {
					  				        	System.err.println("*** error in Heapfile.insertRecord() ***");
					  				        	
					  				        	e.printStackTrace();
					  				              }  
					  			  		}
					  		  		
					  		      }
					  		      catch (Exception e) {
					  			System.err.println("*** Heapfile error in Tuple.setStrFld() ***");
					  			e.printStackTrace();
					  		      }
			  					
			  				}catch(Exception e) {
			  					
			  				}	
		  				
		  				}
		  				else {
		  					// in the case where we have one relation
		  					R2types = R1types;
		  					R2_no_flds = R1_no_flds;
		  					projectionTypes[0] = new AttrType(R1types[q_projection[0].offset].attrType);
		  					projectionTypes[1] = new AttrType(R1types[q_projection[1].offset].attrType);
		  				}
		  				
		  				// construct the predicate condition expression
		  				if((line = query_reader.readLine()) != null)
		  				{
		  					List<String> predicate = Arrays.asList(line.split(" "));
		  					firstPred = new CondExpr[3];
		  					firstPred[0] = new CondExpr();
		  					firstPred[0].next = null;
		  					
		  					// specify operand types
		  					firstPred[0].type1 = new AttrType(AttrType.attrSymbol);
		  					firstPred[0].type2 = new AttrType(AttrType.attrSymbol);
		  					
			  				String operand = predicate.get(0);
			  				index_init = operand.indexOf('_');
			  				operand = operand.substring(index_init+1);
			  				
			  				// specify field specifications for the first operand
			  				firstPred[0].operand1.symbol = new FldSpec (new RelSpec(RelSpec.outer),Integer.parseInt(operand));
		  					
			  				// specify the operator
			  				firstPred[0].op    = new AttrOperator(Integer.parseInt(predicate.get(1)));
			  				
			  				operand = predicate.get(2);
			  				index_init = operand.indexOf('_');
			  				operand = operand.substring(index_init+1);
			  				
			  				// specify field specifications for the second operand
			  				firstPred[0].operand2.symbol = new FldSpec (new RelSpec(RelSpec.innerRel),Integer.parseInt(operand));
			  				
			  				// in the case of a second predicate
			  				if((line = query_reader.readLine()) != null)
			  				{
			  					// set singlePred to false
			  					singlePred = false;
			  					line = query_reader.readLine();
			  					predicate = Arrays.asList(line.split(" "));
			  					
			  					// construct the second predicate
			  					firstPred[1] = new CondExpr();
			  					firstPred[1].next = null;
			  					firstPred[1].type1 = new AttrType(AttrType.attrSymbol);
			  					firstPred[1].type2 = new AttrType(AttrType.attrSymbol);
			  					
				  				operand = predicate.get(0);
				  				index_init = operand.indexOf('_');
				  				operand = operand.substring(index_init+1);
				  				firstPred[1].operand1.symbol = new FldSpec (new RelSpec(RelSpec.outer),Integer.parseInt(operand));
			  					
				  				firstPred[1].op    = new AttrOperator(Integer.parseInt(predicate.get(1)));
				  				
				  				operand = predicate.get(2);
				  				index_init = operand.indexOf('_');
				  				operand = operand.substring(index_init+1);
				  				firstPred[1].operand2.symbol = new FldSpec (new RelSpec(RelSpec.innerRel),Integer.parseInt(operand));
			  				
			  				}
			  				
		  				}
		  				
	  				}
		  		
		  		}
			  		
		      }
		      catch(Exception e){
				System.err.println(e);
				e.printStackTrace();
		      }
	
	}

}


