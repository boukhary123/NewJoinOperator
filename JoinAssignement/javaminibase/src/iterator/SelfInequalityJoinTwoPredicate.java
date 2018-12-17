package iterator;
import java.util.*;   
import heap.*;
import global.*;
import bufmgr.*;
import diskmgr.*;
import index.*;
import java.lang.*;
import java.util.BitSet;
import java.io.*;




// A class to represent an index and field to be sorted 
// the filed is assumed integer and this later can enhanced

class Row 
{ 
	public RID rid; 
	public int field_to_sort; 

	// Constructor 
	public Row(RID rid, int field_to_sort) 
	{ 
		this.rid = new RID(); 
		this.rid.copyRid(rid);
		this.field_to_sort=field_to_sort;

	} 
}	



class SortAsceding implements Comparator<Row> 
{ 
	// Used for sorting in ascending order of 
	// field_to_sort 
	public int compare(Row a, Row b) 
	{ 
		return a.field_to_sort - b.field_to_sort; 
	} 
} 


class SortDesceding implements Comparator<Row> 
{ 
	// Used for sorting in descending order of 
	// field_to_sort 
	public int compare(Row a, Row b) 
	{ 
		return -(a.field_to_sort - b.field_to_sort); 
	} 
} 


/** 
 *
 *  This file contains an implementation of the self join loop with one predicate.
 */

public class SelfInequalityJoinTwoPredicate  extends Iterator 
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
  private   Tuple     outer_tuple, inner_tuple;
  private   Tuple     Jtuple;           // Joined tuple
  private   FldSpec   perm_mat[];
  private   int        nOutFlds;
  private Sort sort_fileds_outer;
  private BitSet BitArray; 
  private int eqoff;
  private int outer_index;
  private int inner_index;
  private ArrayList<Row> L1;
  private ArrayList<Row> L2;
  private int number_of_rows;
  private   Heapfile  hf;
  private   Scan      inner;
  private HashMap<RID, Integer> permutation_array; 


  
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
   *@exception IOException some I/O fault
   *@exception NestedLoopException exception from this class
   */
  public SelfInequalityJoinTwoPredicate( AttrType    in1[],    
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
			   int        n_out_flds
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
      
      // variable to specify sort oder of the file 
      boolean Ascending = false;
      
      try {
	t_size = TupleUtils.setup_op_tuple(Jtuple, Jtypes,
					   in1, len_in1, in2, len_in2,
					   t1_str_sizes, t2_str_sizes,
					   proj_list, nOutFlds);
      }catch (TupleUtilsException e){
	throw new NestedLoopException(e,"TupleUtilsException is caught by NestedLoopsJoins.java");
      }
      
      
      // initialize outer and inner indexes
      outer_index = 0;
      inner_index = 0;
     
      // check how we should sort the L1 array (the heap file)
      if (outFilter[0].op.attrOperator  == AttrOperator.aopGT ||
    		  outFilter[0].op.attrOperator  == AttrOperator.aopGE) {
    	  
    	  
    	  // sort array in ascending order
    	  Ascending = true;
    	   	  
    	 	  
    	  
      }
      
      else if (outFilter[0].op.attrOperator  == AttrOperator.aopLT ||
    		  outFilter[0].op.attrOperator  == AttrOperator.aopLE){
    	  
    	  // sort array in descending order
    	  Ascending = false;
    	    	  
      }
      
      

      
	  
	  try {

		  
		  
		  // initialize L1 array
		  int i=0;
		  L1 = new ArrayList<Row>();
		  L2 = new ArrayList<Row>();

		  hf = new Heapfile(relationName);
		  inner = hf.openScan();
	      RID rid = new RID();
	      int field_to_sort;
	      Row current_row; 
	      
	      // initialize L1 and L2 array
	      while ((inner_tuple = inner.getNext(rid)) != null) {
			  inner_tuple.setHdr((short)in1_len, _in1,t1_str_sizes);
			  // add an element to L1 array
			  field_to_sort=inner_tuple.getIntFld(outFilter[0].operand1.symbol.offset);
			  current_row = new Row(rid, field_to_sort);
	    	  L1.add(current_row);
			  
	    	  // add an element to L2 array
	    	  field_to_sort=inner_tuple.getIntFld(outFilter[1].operand1.symbol.offset);
			  current_row = new Row(rid, field_to_sort);
	    	  L2.add(current_row);
	    	  
	    	  i+=1;
	    	  
	      }
	      
	      
	      // get the number of rows
	      number_of_rows = i;	
	      
	      if (Ascending) {
	    	  Collections.sort(L1, new SortAsceding()); 
	    	  Collections.sort(L2, new SortDesceding()); 

	    	  
	      }
	      
	      else {
	    	  Collections.sort(L1, new SortDesceding());
	    	  Collections.sort(L2, new SortAsceding());	  
	      }
	      
	      // initialize bit array
	      BitArray = new BitSet(number_of_rows);
	      
	      // initialize permutation array
	      permutation_array = new HashMap<RID,Integer>();
	      
	      
	      for (i=0; i<number_of_rows; i++)
	    	  permutation_array.put(L1.get(i).rid,i);

	  }
	  
	  catch (Exception e) {
		  System.err.println ("*** Error preparing for sorting");
		  System.err.println (""+e);
		  Runtime.getRuntime().exit(1);
		  }        
      	  
	  // check if or equal or not
	  if(outFilter[0].op.attrOperator  == AttrOperator.aopGE ||
			  outFilter[0].op.attrOperator  == AttrOperator.aopLE) {
		  
		  eqoff=0;  
	  }
	  
	  else {
		  
		  eqoff=1;
	  }
            
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
    
      while(outer_index<number_of_rows) {
    	  
    	  if(get_from_outer) {
    		  // first parse of this outer index 
    		  // so we need to set inner index
    		  inner_index = 0;
    		  get_from_outer = false;
    	  }
    	  while(inner_index<outer_index-eqoff) {
    		  outer_tuple=L1.get(outer_index);
    		  inner_tuple=L1.get(inner_index);
    		  inner_tuple.setHdr((short)in2_len, _in2,t2_str_sizescopy);
    		  if (PredEval.Eval(RightFilter, inner_tuple, null, _in2, null) == true)
    		  {
    			  if (PredEval.Eval(OutputFilter, outer_tuple, inner_tuple, _in1, _in2) == true)
    			  {
    				  // Apply a projection on the outer and inner tuples.
    				  Projection.Join(outer_tuple, _in1, 
    						  inner_tuple, _in2, 
    						  Jtuple, perm_mat, nOutFlds);
    				  inner_index++;
    				  return Jtuple;
    				}
    			  }
    		  }
    	  inner_index++;
    	  get_from_outer = true;
    	  outer_index++;
    	  } 
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
	  outer.close();
	}catch (Exception e) {
	  throw new JoinsException(e, "NestedLoopsJoin.java: error in closing iterator.");
	}
	closeFlag = true;
      }
    }
}






