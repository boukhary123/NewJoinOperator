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


/** 
*
*  This file contains an implementation of the Inequality join loop with two predicates.
*/

public class InequalityJoinTwoPredicates  extends Iterator 
{
  private AttrType      _in1[],  _in2[];
  private   int        in1_len, in2_len;
  private   short t2_str_sizescopy[];
  private   CondExpr OutputFilter[];
  private   CondExpr RightFilter[];
  private   int        n_buf_pgs;        // # of buffer pages available.
  private   boolean   get_from_outer;    // if TRUE, a tuple is got from outer
  private   Tuple     outer_tuple, inner_tuple;
  private   Tuple     Jtuple;           // Joined tuple
  private   FldSpec   perm_mat[];
  private   int        nOutFlds;
  private BitSet BitArray; 
  private int eqoff;
  private int i_index,j_index,k_index;
  private ArrayList<Row> L1;
  private ArrayList<RowWithTuple> L2;
  private ArrayList<RowWithTuple> L1_prime;
  private ArrayList<Row> L2_prime; 
  private int number_of_rows,number_of_rows_prime;
  private   Heapfile  hf1,hf2;
  private   Scan      outer,inner;
  private int permutation_array[],permutation_array_prime[],offset1[],offset2[],off1,off2;
  //private ArrayList<Integer> offset1,offset2;


  
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
   *@param relationName  access heap file for right i/p to join
   *@param outFilter   select expressions
   *@param rightFilter reference to filter applied on right i/p
   *@param proj_list shows what input fields go where in the output tuple
   *@param n_out_flds number of outer relation fields
   *@exception IOException some I/O fault
   *@exception NestedLoopException exception from this class
   */
  public InequalityJoinTwoPredicates( AttrType    in1[],    
			   int     len_in1,           
			   short   t1_str_sizes[],
			   AttrType    in2[],         
			   int     len_in2,           
			   short   t2_str_sizes[],   
			   int     amt_of_mem,        
			   Iterator     am1,          
			   String relationName1,
			   String relationName2,
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
      get_from_outer = true;
      
      AttrType[] Jtypes = new AttrType[n_out_flds];
      short[]    t_size;
      
      perm_mat = proj_list;
      nOutFlds = n_out_flds;
      
      // variable to specify sort oder of the array 
      boolean Descending_op1 = false;
      boolean Ascending_op2 = false;
      
      try {
    	  t_size = TupleUtils.setup_op_tuple(Jtuple, Jtypes,
					   in1, len_in1, in2, len_in2,
					   t1_str_sizes, t2_str_sizes,
					   proj_list, nOutFlds);
    	  
    	  }catch (TupleUtilsException e) {
    		  throw new NestedLoopException(
    				  e,"TupleUtilsException is caught by SelfInequalityjoin.java");
    		  }
      
     i_index=0;
     j_index=0;
     k_index=0;
     
      // check how we should sort the L1 array and L2 array
      if (outFilter[0].op.attrOperator  == AttrOperator.aopGT ||
    		  outFilter[0].op.attrOperator  == AttrOperator.aopGE) {
    	  
    	  Descending_op1 = true;
    	   	 	  
      }
      
      else if (outFilter[0].op.attrOperator  == AttrOperator.aopLT ||
    		  outFilter[0].op.attrOperator  == AttrOperator.aopLE){
    	  
    	  Descending_op1 = false;
      }
      
      if(outFilter[1].op.attrOperator  == AttrOperator.aopGT ||
    		  outFilter[1].op.attrOperator  == AttrOperator.aopGE) {
    	  
    	  Ascending_op2 = true;  
      }
	  
      
      else if(outFilter[1].op.attrOperator  == AttrOperator.aopLT ||
    		  outFilter[1].op.attrOperator  == AttrOperator.aopLE) {
    	  
    	  Ascending_op2 = false;  
      }
      
	  try {	  
		  
		  int i=0;
		  L1 = new ArrayList<Row>();
		  L2 = new ArrayList<RowWithTuple>();
		  L1_prime = new ArrayList<RowWithTuple>();
		  L2_prime = new ArrayList<Row>(); 

		  hf1 = new Heapfile(relationName1);
		  outer = hf1.openScan();
		  hf2 = new Heapfile(relationName2);
		  inner= hf2.openScan();
	      RID rid = new RID();
	      int field_to_sort;
	      FldSpec   perm[], perm_prime[];
	      perm = new FldSpec[1];
	      perm[0] = new FldSpec(perm_mat[0].relation,perm_mat[0].offset);
	      perm_prime = new FldSpec[1];
	      perm_prime[0]= new FldSpec(perm_mat[1]); // not sure to be checked
	      perm_prime[0].relation.key=0;
 
	      // initialize L1 and L2 array
	      while ((outer_tuple = outer.getNext(rid)) != null) {
			  outer_tuple.setHdr((short)in1_len, _in1,t1_str_sizes);
			  
			  // add an element to L2 array
			  field_to_sort=outer_tuple.getIntFld(outFilter[0].operand1.symbol.offset);
			  Projection.Project(outer_tuple, _in1, Jtuple, perm,1);
	    	  L1.add(new Row(rid, field_to_sort));
			  
	    	  // add an element to L1 array
	    	  field_to_sort=outer_tuple.getIntFld(outFilter[1].operand1.symbol.offset);
			  Projection.Project(outer_tuple, _in1, Jtuple, perm,1);
	    	  L2.add(new RowWithTuple(rid, field_to_sort,Jtuple));

	    	  
	    	  // keep track of the number of rows
	    	  i+=1;
	    	  
	      }
	      
	      number_of_rows = i;	
	      i=0;

	      
	      // initialize L1 and L2 array
	      while ((inner_tuple = inner.getNext(rid)) != null) {
			  inner_tuple.setHdr((short)in2_len, _in2,t2_str_sizes);
			  
			  // add an element to L2 array
			  field_to_sort=inner_tuple.getIntFld(outFilter[0].operand2.symbol.offset);
			  Projection.Project(inner_tuple, _in2, Jtuple, perm_prime,1);
	    	  L1_prime.add(new RowWithTuple(rid, field_to_sort,Jtuple));
			  
	    	  // add an element to L1 array
	    	  field_to_sort=inner_tuple.getIntFld(outFilter[1].operand2.symbol.offset);
			  Projection.Project(inner_tuple, _in2, Jtuple, perm_prime,1);
	    	  L2_prime.add(new Row(rid, field_to_sort));
	    	  
	    	  
	    	  // keep track of the number of rows
	    	  i+=1;
	    	  
	      }

	      number_of_rows_prime = i;	
	      
	      if (Descending_op1) {
	    	  Collections.sort(L1, new SortDesceding()); 
	    	  Collections.sort(L1_prime, new SortDesceding()); 

	      }
	      
	      else {
	    	  Collections.sort(L1, new SortAsceding());
	    	  Collections.sort(L1_prime, new SortAsceding());

	      }
	      
	      if(Ascending_op2) {
	    	  Collections.sort(L2, new SortAsceding());
	    	  Collections.sort(L2_prime, new SortAsceding());

	      }
	      
	      else {
	    	  Collections.sort(L2, new SortDesceding());   
	    	  Collections.sort(L2_prime, new SortDesceding());   
	      }
	      
	      // initialize bit array
	      BitArray = new BitSet(number_of_rows_prime);
	      
	      // initialize permutation array
	      permutation_array = new int[number_of_rows]; 
	      permutation_array_prime = new int[number_of_rows_prime]; 
	      
	      for (i=0; i<number_of_rows; i++)
	    	  for (int j=0;j<number_of_rows;j++) {
	    		  if (L2.get(i).equals(L1.get(j))){
	    			  permutation_array[i]=j;
	    			  break;
	    		  }
	    	  }
	      
	      for (i=0; i<number_of_rows_prime; i++)
	    	  for (int j=0;j<number_of_rows_prime;j++) {
	    		  if (L2_prime.get(i).equals(L1_prime.get(j))){
	    			  permutation_array_prime[i]=j;
	    			  break;
	    		  }
	    	  }
	      
	      
	      // compute the offset arrays

	      offset1 = new  int[number_of_rows];
	      offset2 = new int[number_of_rows];
	      SortMerge(L1,L1_prime,offset1,!Descending_op1);
	      SortMergeWithTuple(L2,L2_prime,offset2,Ascending_op2);
	      
   
	  }
	  
	  catch (Exception e) {
		  System.err.println ("*** Error preparing for sorting");
		  System.err.println (""+e);
		  Runtime.getRuntime().exit(1);
		  }        
      	  
	  // check if or equal or not
	  if((outFilter[0].op.attrOperator  == AttrOperator.aopGE ||
			  outFilter[0].op.attrOperator  == AttrOperator.aopLE) &&
			  (outFilter[1].op.attrOperator  == AttrOperator.aopGE ||
			  outFilter[1].op.attrOperator  == AttrOperator.aopLE)){
		  
		  eqoff=0;  
	  }
	  
	  else {
		  
		  eqoff=1;
	  }
            
    }
  
  
  public void SortMerge(ArrayList<Row> arr1,
		  ArrayList<RowWithTuple> arr2, int offset[], boolean Ascending) {
	  int m = arr2.size();
	  int n = arr1.size();
	  int i=0,j=0;

	  while(i < n) {
    	  
    	  if(arr1.get(i).field_to_sort == arr2.get(j).field_to_sort) {
    		  
    		  offset[i]=j;
    		  i++;
    		  
    		  if (j+1 < m)
    			  j++;
    		  else 
    			  j = m-1;
    	  }
    	  
    	  else if(Ascending? arr1.get(i).field_to_sort<arr2.get(j).field_to_sort:
    		  arr1.get(i).field_to_sort>arr2.get(j).field_to_sort) {
    		  offset[i]=j-1;
    		  i++;
    	  }
    	  
    	  else {
    		  if (j+1 < m)
    			  j++;
    		  
    		  else { 
    			  j = m-1;
    			  offset[i]=j+1;
    			  i++;
    		  }
    	  }
      }
  }
  
  
  public void SortMergeWithTuple(ArrayList<RowWithTuple> arr1,
		  ArrayList<Row> arr2, int offset[], boolean Ascending) {
	  int m = arr2.size();
	  int n = arr1.size();
	  int i=0,j=0;

	  while(i < n) {
    	  
    	  if(arr1.get(i).field_to_sort == arr2.get(j).field_to_sort) {
    		  
    		  offset[i]=j;
    		  i++;
    		  
    		  if (j+1 < m)
    			  j++;
    		  else 
    			  j = m-1;
    	  }
    	  
    	  else if(Ascending? arr1.get(i).field_to_sort<arr2.get(j).field_to_sort:
    		  arr1.get(i).field_to_sort>arr2.get(j).field_to_sort) {
    		  offset[i]=j-1;
    		  i++;
    	  }
    	  
    	  else {
    		  if (j+1 < m)
    			  j++;
    		  
    		  else { 
    			  j = m-1;
    			  offset[i]=j+1;
    			  i++;
    		  }
    	  }
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
    
      while(i_index<number_of_rows) {
    	  
    	  if(get_from_outer) {
    		  // first parse of this outer index 
    		  // so we need to set inner index and current bit array position
    		  off2 = offset2[i_index];
    		  for(j_index=0;j_index< Math.min(off2+1,L2_prime.size());j_index++){
    			  
    			  BitArray.set(permutation_array_prime[j_index]);
    		  }
    		  
    		  off1 = offset1[permutation_array[i_index]];
    		  k_index = off1+eqoff;
    		  get_from_outer = false;

    	  }
    	  
    	  while(k_index<number_of_rows_prime) {
    		  
    		  if(BitArray.get(k_index)) {
    			  outer_tuple = L2.get(i_index).field_to_select;
    			  inner_tuple = L1_prime.get(k_index).field_to_select;
    			  
    			  // join both tuples into one tuple and return it
    			  Projection.Join(outer_tuple, _in1,inner_tuple, _in2, 
    					  Jtuple, perm_mat, nOutFlds);
    			  k_index++;
    			  return Jtuple;
    			  
    		  }
    		  
    		  k_index++;
    	  }
    	  
    	  get_from_outer = true;
    	  i_index++;
    	  
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
	  outer.closescan();
	  inner.closescan();
	}catch (Exception e) {
	  throw new JoinsException(e, "NestedLoopsJoin.java: error in closing iterator.");
	}
	closeFlag = true;
      }
    }
}





