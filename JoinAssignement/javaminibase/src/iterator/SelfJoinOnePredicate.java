package iterator;
import java.util.*;   
import heap.*;
import global.*;
import bufmgr.*;
import diskmgr.*;
import index.*;
import java.lang.*;
import java.io.*;

/** 
 *
 *  This file contains an implementation of the self join loop with one predicate.
 */

public class SelfJoinOnePredicate  extends Iterator 
{
  private AttrType      _in1[],  _in2[];
  private   int        in1_len, in2_len;
  private   Iterator  outer;
  private   short t2_str_sizescopy[];
  private   CondExpr OutputFilter[];
  private   CondExpr RightFilter[];
  private   int        n_buf_pgs;        // # of buffer pages available.
  private   boolean        done,         // Is the join complete
    get_from_outer;                 // if TRUE, a tuple is got from outer
  private   Tuple     inner_tuple,outer_tuple;
  private   Tuple     Jtuple;           // Joined tuple
  private   FldSpec   perm_mat[];
  private   int        nOutFlds;
  //private Sort sort_fileds_outer;
  private int eqoff;
  private int outer_index;
  private int inner_index;
  private ArrayList<RowWithTuple> L1;
  private int number_of_rows;
  private   Heapfile  hf;
  private   Scan      inner;
  
  
  /**constructor
   *Initialize the two relations which are joined, including relation type,
   *@param in1  Array containing field types of R.
   *@param len_in1  # of columns in R.
   *@param t1_str_sizes shows the length of the string fields.
   *@param in2  Array containing field types of S
   *@param len_in2  # of columns in S
   *@param  t2_str_sizes shows the length of the string fields.
   *@param amt_of_mem  IN PAGES
   *@param am1  access method for left i/p to join
   *@param relationName  access heapfile for right i/p to join
   *@param outFilter   select expressions
   *@param rightFilter reference to filter applied on right i/p
   *@param proj_list shows what input fields go where in the output tuple
   *@param n_out_flds number of outer relation fields
   *@param file_path path of the text file containing the records
   *@exception IOException some I/O fault
   *@exception NestedLoopException exception from this class
   */
  public SelfJoinOnePredicate( AttrType    in1[],    
			   int     len_in1,           
			   short   t1_str_sizes[],
			   AttrType    in2[],         
			   int     len_in2,           
			   short   t2_str_sizes[],   
			   int     amt_of_mem,        
			   Iterator     am1,          
			   String relationName,      
			   CondExpr outFilter[],      
			   CondExpr rightFilter[],    
			   FldSpec   proj_list[],
			   int        n_out_flds,
			   String file_path
			   ) throws IOException,NestedLoopException
    {
	  

	  _in1 = new AttrType[in1.length];
      _in2 = new AttrType[in2.length];
      System.arraycopy(in1,0,_in1,0,in1.length);
      System.arraycopy(in2,0,_in2,0,in2.length);
      in1_len = len_in1;
      in2_len = len_in2;
      t2_str_sizescopy =  t2_str_sizes;
      inner_tuple = new Tuple();
      Jtuple = new Tuple();
      OutputFilter = outFilter;
      RightFilter  = rightFilter;
      
      n_buf_pgs    = amt_of_mem;
      done  = false;
      get_from_outer = true;
      
      AttrType[] Jtypes = new AttrType[n_out_flds];
      short[]    t_size;
      
      perm_mat = proj_list;
      nOutFlds = n_out_flds;
     
      // variable to specify sort oder of the array 
      boolean Ascending_op1 = false;
      
      try {
    	  t_size = TupleUtils.setup_op_tuple(Jtuple, Jtypes,
					   in1, len_in1, in2, len_in2,
					   t1_str_sizes, t2_str_sizes,
					   proj_list, nOutFlds);
      }
      catch (TupleUtilsException e){
    	  throw new NestedLoopException(
    			  e,"TupleUtilsException is caught by SelfJoinOnrPredicate.java");
      }
      
      
      // initialize outer and inner indexes
      outer_index = 0;
      inner_index = 0;
      
      // set up the reader that will read the records from the text file
      this.firstHeapFileCall = true;
      this.reader = new BufferedReader(new FileReader("../../"+file_path+".txt"));
                
      // check how we should sort the L1 array (the heap file)
      if (outFilter[0].op.attrOperator  == AttrOperator.aopGT ||
    		  outFilter[0].op.attrOperator  == AttrOperator.aopGE) {
    	  
    	  
    	  // sort array in ascending order
    	  Ascending_op1 = true;
      }
      
      else if (outFilter[0].op.attrOperator  == AttrOperator.aopLT ||
    		  outFilter[0].op.attrOperator  == AttrOperator.aopLE){
    	  
    	  // sort array in descending order
    	  Ascending_op1 = false;
    	  
      }
      
	  
	  try {

		  
		  int i=0;
		  
		  // array that will store tuples and field to be sorted
		  L1 = new ArrayList<RowWithTuple>();
		  
		  // heap file containing the relations
		  //hf = new Heapfile(relationName);
		  
		  // scan iterator to read elements from heap file 
		  //inner = hf.openScan();
		  
		  // this variable is used to keep track of the record id of the current tuple
	      RID rid = new RID();
	      
	      // this variable is used to keep track of the field
	      // to be sorted of the current tuple being read 
	      int field_to_sort;
	      FldSpec   perm[];
	      
	      // perm is used to project the read tuples from the heapfile
	      // on the field to be selected to avoid unnecessary data being stored in the array 
	      perm = new FldSpec[1];
	      perm[0]=perm_mat[0];
 
	      
	      
		while(true)
		{
			if(!this.getNextHeapFile(len_in1, _in1,relationName)) {
				
				hf = new Heapfile(relationName);
				inner = hf.openScan();

			    try {
				      // read tuples from the heap file and fill the L1 array
				      while ((inner_tuple = inner.getNext(rid)) != null) {
				    	  // set the header of the read tuple
						  inner_tuple.setHdr((short)in1_len, _in1,t1_str_sizes);
						  
						  // set the field to be sorted
						  field_to_sort=inner_tuple.getIntFld(outFilter[0].operand1.symbol.offset);
						  
						  // project the tuple on the field to be selected finally
						  Projection.Project(inner_tuple, _in1, Jtuple, perm,1);
						  
						  // add element to L1 array
				    	  L1.add(new RowWithTuple(rid, field_to_sort,Jtuple));
						  	    	  
				    	  // keep track of the number of rows
				    	  i+=1;
				    	  
				      }

			    }
			    catch (Exception e) {
			      System.err.println (""+e);
			      e.printStackTrace();
			      Runtime.getRuntime().exit(1);
			    }
			    
			    Heapfile F = new Heapfile(relationName);
			    F.deleteFile();
			}else {

			    Tuple t = new Tuple();
			    t = null;
				hf = new Heapfile(relationName);
				inner = hf.openScan();
			    try {
				      // read tuples from the heap file and fill the L1 array
				      while ((inner_tuple = inner.getNext(rid)) != null) {
				    	  // set the header of the read tuple
						  inner_tuple.setHdr((short)in1_len, _in1,t1_str_sizes);
						  
						  // set the field to be sorted
						  field_to_sort=inner_tuple.getIntFld(outFilter[0].operand1.symbol.offset);
						  
						  // project the tuple on the field to be selected finally
						  Projection.Project(inner_tuple, _in1, Jtuple, perm,1);
						  
						  // add element to L1 array
				    	  L1.add(new RowWithTuple(rid, field_to_sort,Jtuple));
						  	    	  
				    	  // keep track of the number of rows
				    	  i+=1;
				    	  
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
	      
//	      // read tuples from the heap file and fill the L1 array
//	      while ((inner_tuple = inner.getNext(rid)) != null) {
//	    	  // set the header of the read tuple
//			  inner_tuple.setHdr((short)in1_len, _in1,t1_str_sizes);
//			  
//			  // set the field to be sorted
//			  field_to_sort=inner_tuple.getIntFld(outFilter[0].operand1.symbol.offset);
//			  
//			  // project the tuple on the field to be selected finally
//			  Projection.Project(inner_tuple, _in1, Jtuple, perm,1);
//			  
//			  // add element to L1 array
//	    	  L1.add(new RowWithTuple(rid, field_to_sort,Jtuple));
//			  	    	  
//	    	  // keep track of the number of rows
//	    	  i+=1;
//	    	  
//	      }
	      
			 
		 // get the number of rows
		 number_of_rows = i;	
		 
		 // sort array according to sort order
	      if (Ascending_op1) {
	    	  Collections.sort(L1, new SortAsceding()); 
	      }
	      
	      else {
	    	  Collections.sort(L1, new SortDesceding());
	      }
		 
		 
		 
	  }
	  
	  catch (Exception e) {
		  System.err.println ("*** Error preparing for sorting");
		  System.err.println (""+e);
		  Runtime.getRuntime().exit(1);
		  }        
      	  
	  // eqoff variable is used to detect if we have equal operator
	  if(outFilter[0].op.attrOperator  == AttrOperator.aopGE ||
			  outFilter[0].op.attrOperator  == AttrOperator.aopLE) {
		  eqoff=1;  
	  }
	  
	  else {
		  
		  eqoff=0;
	  }     
    }
  
  
  /**
   * This function is used to compares 2 fields according to a specific operator
   * 
   * @param field1 The first integer field to compare 
   * @param field2 The second integer field to compare
   * @param operator_type integer specifying the type of operator
   * @return true if (field op1 field2)
   */
  public static boolean predicate_evaluate(int field1,int field2,int operator_type) {
	  
	  // return the truth value of field1 op field2
	  switch(operator_type) {
	  case 1:
		  return field1<field2;
	  case 2:
		  return field1>field2;
		  
	  case 4:
		  return field1<=field2;
		  
	  case 5:	  
		  return field1>= field2;  
	  }
	return false;
	  
  }
  
  /**  
   *@return The joined tuple is returned
   *@exception IOException I/O errors
   *@exception JoinsException some join exception
   *@exception IndexException exception from super class
   *@exception InvalidTupleSizeException invalid tuple size
   *@exception InvalidTypeException tuple type not valid
   *@exception PageNotReadException exception from lower layer
   *@exception TupleUtilsException exception from using tuple utilities
   *@exception PredEvalException exception from PredEval class
   *@exception SortException sort exception
   *@exception LowMemException memory error
   *@exception UnknowAttrType attribute type unknown
   *@exception UnknownKeyTypeException key type unknown
   *@exception Exception other exceptions

   */
  public Tuple get_next()
    throws IOException,
	   JoinsException ,
	   IndexException,
	   InvalidTupleSizeException,
	   InvalidTypeException, 
	   PageNotReadException,
	   TupleUtilsException, 
	   PredEvalException,
	   SortException,
	   LowMemException,
	   UnknowAttrType,
	   UnknownKeyTypeException,
	   Exception
    {
    
	  int outer_tuple_fld1,inner_tuple_fld1;
	  
      while(outer_index<number_of_rows) {
    	  
    	  if(get_from_outer) {
    		  // first parse of this outer index 
    		  // so we need to set inner index to zero
    		  inner_index = 0;
    		  get_from_outer = false;
    	  }
  
    	  while(inner_index<outer_index+eqoff) {
    		  
    		  // get outer and inner tuple
    		  outer_tuple=L1.get(outer_index).field_to_select;
    		  inner_tuple=L1.get(inner_index).field_to_select;
    		  
    		  // retrieve fields to compare them to avoid duplicates being returned 
    		  outer_tuple_fld1 = L1.get(outer_index).field_to_sort;
    		  inner_tuple_fld1= L1.get(inner_index).field_to_sort;

    		  // this function is used here to deal with duplicates it checks if the 
    		  // 2 tuples satisfy the join predicate

    		  if(SelfJoinOnePredicate.predicate_evaluate(
					  outer_tuple_fld1, inner_tuple_fld1, OutputFilter[0].op.attrOperator)) {
	    		  
    			  // join inner and outer tuples
	    		  Projection.Join(outer_tuple, _in1, inner_tuple, _in2, 
	    				  Jtuple, perm_mat, nOutFlds);
	    		  inner_index++;
	    		  return Jtuple;
	    		  } 
    		  inner_index++;
    		  }
    	  
    	  get_from_outer = true;
    	  outer_index++;
    	  } 
      // all elements are finished
      return null;
      
      }
 
  /**
   * implement the abstract method close() from super class Iterator
   *to finish cleaning up
   *@exception IOException I/O error from lower layers
   *@exception JoinsException join error from lower layers
   *@exception IndexException index access error 
   */
  public void close() throws JoinsException, IOException,IndexException 
    {
      if (!closeFlag) {
	
	try {
		inner.closescan();
	}catch (Exception e) {
	  throw new JoinsException(e, "NestedLoopsJoin.java: error in closing iterator.");
	}
	closeFlag = true;
      }
    }
}






