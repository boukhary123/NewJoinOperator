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
	
	List<String> relations;
	
	FldSpec []  projection; 
	AttrType[] projectionTypes;
	
	AttrType[] R1types;
	AttrType[] R2types;
	
	int R1_no_flds;
	int R2_no_flds;
	
	boolean singlePred;
	
	CondExpr[] firstPred;
	CondExpr[] secondPred;
	
	Heapfile R1_hf;
	Heapfile R2_hf;
	

	
	public QueryParser(File f)
	{
		singlePred = true;
		
		try {
		  		
				BufferedReader query_reader = new BufferedReader(new FileReader(f)); 
		  		
		  		String line;
		  		
		  		if((line = query_reader.readLine()) != null)
		  		{
		  			List<String> Select = Arrays.asList(line.split(" "));
		  			
		  			projection = new FldSpec[2];
		  			
		  			projectionTypes = new AttrType[2];
		  			
	  				String field = Select.get(0);
	  				int index_init = field.indexOf('_');
	  				field = field.substring(index_init+1);
	  				
	  				projection[0] = new FldSpec(new RelSpec(RelSpec.outer), Integer.parseInt(field));
	  				
	  				field = Select.get(1);
	  				index_init = field.indexOf('_');
	  				field = field.substring(index_init+1);
	  				
	  				projection[1] = new FldSpec(new RelSpec(RelSpec.innerRel), Integer.parseInt(field));
	  				
	  				if((line = query_reader.readLine()) != null)
	  				{
	  					relations = Arrays.asList(line.split(" "));
			  			
	  					int no_relations = relations.size();
	  					
		  				String relation = relations.get(0);
		  				
		  				File rel_file = new File("../../"+relation+".txt");
		  				
		  				try {
		  					BufferedReader rel_reader = new BufferedReader(new FileReader(rel_file));
		  					String header;
		  					if((header = rel_reader.readLine()) != null) {
					  			List<String> attributes = Arrays.asList(header.split(","));
					  			R1_no_flds = attributes.size();
					  			R1types = new AttrType[R1_no_flds];
					  			for(int i = 0; i< R1_no_flds;i++) {
					  				switch(attributes.get(i)) {
					  				case "attrString":
					  					R1types[i] = new AttrType(0);
					  					break;
					  				case "attrInteger":
					  					R1types[i] = new AttrType(1);
					  					break;
					  				case "attrReal":
					  					R1types[i] = new AttrType(2);
					  					break;
					  				case "attrSymbol":
					  					R1types[i] = new AttrType(3);
					  					break;
					  				case "attrNull":
					  					R1types[i] = new AttrType(4);
					  					break;
					  				default:
					  					R1types[i] = new AttrType(4);
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
					  		    
					  		    // inserting the tuple into file "sailors"
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
					  		    	  
//					  			t.setIntFld(1, ((Sailor)sailors.elementAt(i)).sid);
//					  			t.setStrFld(2, ((Sailor)sailors.elementAt(i)).sname);
//					  			t.setIntFld(3, ((Sailor)sailors.elementAt(i)).rating);
//					  			t.setFloFld(4, (float)((Sailor)sailors.elementAt(i)).age);
					  					
					  			  		BufferedReader br = new BufferedReader(new FileReader(rel_file)); 
					  			  		
					  			  		String rec;
					  			  		rec = br.readLine();
					  			  		while((rec = br.readLine()) != null) {
					  			  			List<String> fields = Arrays.asList(rec.split(","));		  		
					  			  			t.setIntFld(1, Integer.parseInt(fields.get(0)));
					  				  		t.setIntFld(2, Integer.parseInt(fields.get(1)));
					  				  		t.setIntFld(3, Integer.parseInt(fields.get(2)));
					  				  		t.setIntFld(4, Integer.parseInt(fields.get(3)));
					  				  		
					  				        try {
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
		  				
		  				if (no_relations > 1)
		  				{
		  					projectionTypes[0] = new AttrType(R1types[projection[0].offset].attrType);
		  					projectionTypes[1] = new AttrType(R2types[projection[1].offset].attrType);
		  					
		  					relation = relations.get(1);
			  				
			  				rel_file = new File("../../"+relation+".txt");
			  				
			  				try {
			  					BufferedReader rel_reader = new BufferedReader(new FileReader(rel_file));
			  					String header;
			  					if((header = rel_reader.readLine()) != null) {
						  			List<String> attributes = Arrays.asList(header.split(","));
						  			R2_no_flds = attributes.size();
						  			R2types = new AttrType[R2_no_flds];
						  			for(int i = 0; i< R2_no_flds;i++) {
						  				switch(attributes.get(i)) {
						  				case "attrString":
						  					R2types[i] = new AttrType(0);
						  					break;
						  				case "attrInteger":
						  					R2types[i] = new AttrType(1);
						  					break;
						  				case "attrReal":
						  					R2types[i] = new AttrType(2);
						  					break;
						  				case "attrSymbol":
						  					R2types[i] = new AttrType(3);
						  					break;
						  				case "attrNull":
						  					R2types[i] = new AttrType(4);
						  					break;
						  				default:
						  					R2types[i] = new AttrType(4);
						  				}
						  			}
		
			  					}
			  					
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
					  					
					  			  		BufferedReader br = new BufferedReader(new FileReader(rel_file)); 
					  			  		
					  			  		String rec;
					  			  		rec = br.readLine();
					  			  		while((rec = br.readLine()) != null) {
					  			  			List<String> fields = Arrays.asList(rec.split(","));		  		
					  			  			t.setIntFld(1, Integer.parseInt(fields.get(0)));
					  				  		t.setIntFld(2, Integer.parseInt(fields.get(1)));
					  				  		t.setIntFld(3, Integer.parseInt(fields.get(2)));
					  				  		t.setIntFld(4, Integer.parseInt(fields.get(3)));
					  				  		
					  				        try {
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
		  					R2types = R1types;
		  					R2_no_flds = R1_no_flds;
		  					projectionTypes[0] = new AttrType(R1types[projection[0].offset].attrType);
		  					projectionTypes[1] = new AttrType(R1types[projection[1].offset].attrType);
		  				}
		  				
		  				if((line = query_reader.readLine()) != null)
		  				{
		  					List<String> predicate = Arrays.asList(line.split(" "));
		  					firstPred = new CondExpr[2];
		  					firstPred[0] = new CondExpr();
		  					firstPred[1] = null;
		  					firstPred[0].next  = null;
		  					firstPred[0].type1 = new AttrType(AttrType.attrSymbol);
		  					firstPred[0].type2 = new AttrType(AttrType.attrSymbol);
		  					
			  				String operand = predicate.get(0);
			  				index_init = operand.indexOf('_');
			  				operand = operand.substring(index_init+1);
			  				firstPred[0].operand1.symbol = new FldSpec (new RelSpec(RelSpec.outer),Integer.parseInt(field));
		  					
			  				firstPred[0].op    = new AttrOperator(Integer.parseInt(predicate.get(1)));
			  				
			  				operand = predicate.get(2);
			  				index_init = operand.indexOf('_');
			  				operand = operand.substring(index_init+1);
			  				firstPred[0].operand1.symbol = new FldSpec (new RelSpec(RelSpec.innerRel),Integer.parseInt(operand));
			  				
			  				if((line = query_reader.readLine()) != null && (line = query_reader.readLine()) == "AND")
			  				{
			  					singlePred = false;
			  					line = query_reader.readLine();
			  					predicate = Arrays.asList(line.split(" "));
			  					secondPred = new CondExpr[2];
			  					secondPred[0] = new CondExpr();
			  					secondPred[1] = null;
			  					secondPred[0].next  = null;
			  					secondPred[0].type1 = new AttrType(AttrType.attrSymbol);
			  					secondPred[0].type2 = new AttrType(AttrType.attrSymbol);
			  					
				  				operand = predicate.get(0);
				  				index_init = operand.indexOf('_');
				  				operand = operand.substring(index_init+1);
				  				secondPred[0].operand1.symbol = new FldSpec (new RelSpec(RelSpec.outer),Integer.parseInt(operand));
			  					
				  				secondPred[0].op    = new AttrOperator(Integer.parseInt(predicate.get(1)));
				  				
				  				operand = predicate.get(2);
				  				index_init = operand.indexOf('_');
				  				operand = operand.substring(index_init+1);
				  				secondPred[0].operand1.symbol = new FldSpec (new RelSpec(RelSpec.innerRel),Integer.parseInt(operand));
			  				
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


