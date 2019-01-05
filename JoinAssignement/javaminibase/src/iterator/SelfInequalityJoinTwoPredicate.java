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
 * This file contains an implementation of the self join loop with two
 * predicates.
 */

public class SelfInequalityJoinTwoPredicate extends Iterator {
	private AttrType _in1[], _in2[];
	private int in1_len, in2_len;
	private Iterator outer;
	private short t2_str_sizescopy[];
	private CondExpr OutputFilter[];
	private CondExpr RightFilter[];
	private int n_buf_pgs; // # of buffer pages available.
	private boolean get_from_outer; // if TRUE, a tuple is got from outer
	private Tuple outer_tuple, inner_tuple;
	private Tuple Jtuple; // Joined tuple
	private FldSpec perm_mat[];
	private int nOutFlds;
	private BitSet BitArray;
	private int eqoff;
	private int outer_index;
	private int inner_index;
	private ArrayList<RowWithTuple> L1;
	private ArrayList<Row> L2;
	private int number_of_rows;
	private Heapfile hf;
	private Scan inner;
	private int permutation_array[], permutation_array_1_to_2[];
	int pos;

	/**
	 * constructor Initialize the two relations which are joined, including relation
	 * type,
	 * 
	 * @param in1          Array containing field types of R.
	 * @param len_in1      # of columns in R.
	 * @param t1_str_sizes shows the length of the string fields.
	 * @param in2          Array containing field types of S
	 * @param len_in2      # of columns in S
	 * @param t2_str_sizes shows the length of the string fields.
	 * @param amt_of_mem   IN PAGES
	 * @param am1          access method for left i/p to join
	 * @param relationName access heap file for right i/p to join
	 * @param outFilter    select expressions
	 * @param rightFilter  reference to filter applied on right i/p
	 * @param proj_list    shows what input fields go where in the output tuple
	 * @param n_out_flds   number of outer relation fields
	 * @param file_path    path of the text file containing the records
	 * @exception IOException         some I/O fault
	 * @exception NestedLoopException exception from this class
	 */
	public SelfInequalityJoinTwoPredicate(AttrType in1[], int len_in1, short t1_str_sizes[], AttrType in2[],
			int len_in2, short t2_str_sizes[], int amt_of_mem, Iterator am1, String relationName, CondExpr outFilter[],
			CondExpr rightFilter[], FldSpec proj_list[], int n_out_flds, String file_path)
			throws IOException, NestedLoopException {
		_in1 = new AttrType[in1.length];
		_in2 = new AttrType[in2.length];
		System.arraycopy(in1, 0, _in1, 0, in1.length);
		System.arraycopy(in2, 0, _in2, 0, in2.length);
		in1_len = len_in1;
		in2_len = len_in2;
		t2_str_sizescopy = t2_str_sizes;
		inner_tuple = new Tuple();
		Jtuple = new Tuple();
		OutputFilter = outFilter;
		RightFilter = rightFilter;

		n_buf_pgs = amt_of_mem;
		get_from_outer = true;

		AttrType[] Jtypes = new AttrType[n_out_flds];
		short[] t_size;

		perm_mat = proj_list;
		nOutFlds = n_out_flds;

		// variable to specify sort oder of the array
		boolean Ascending_op1 = false;
		boolean Descending_op2 = false;

		try {
			t_size = TupleUtils.setup_op_tuple(Jtuple, Jtypes, in1, len_in1, in2, len_in2, t1_str_sizes, t2_str_sizes,
					proj_list, nOutFlds);

		} catch (TupleUtilsException e) {
			throw new NestedLoopException(e, "TupleUtilsException is caught by SelfInequalityjoin.java");
		}

		// initialize outer and inner indexes
		outer_index = 0;
		inner_index = 0;
		
		// set up the reader that will read the records from the text file
		this.firstHeapFileCall = true;
		this.reader = new BufferedReader(new FileReader("../../../" + file_path + ".txt"));

		// check how we should sort the L1 array and L2 array
		if (outFilter[0].op.attrOperator == AttrOperator.aopGT || outFilter[0].op.attrOperator == AttrOperator.aopGE) {

			Ascending_op1 = true;

		}

		else if (outFilter[0].op.attrOperator == AttrOperator.aopLT
				|| outFilter[0].op.attrOperator == AttrOperator.aopLE) {

			Ascending_op1 = false;
		}

		if (outFilter[1].op.attrOperator == AttrOperator.aopGT || outFilter[1].op.attrOperator == AttrOperator.aopGE) {

			Descending_op2 = true;
		}

		else if (outFilter[1].op.attrOperator == AttrOperator.aopLT
				|| outFilter[1].op.attrOperator == AttrOperator.aopLE) {

			Descending_op2 = false;
		}

		try {

			// this variable is used to make sure record id's are unique for tuples 
			// from different heapfile
			int heapfile_index = 0;

			// index to keep track of the total number of tuples
			int i = 0;
			// we only return elements from L1 array so we need to store only in L1
			// the tuples to be returned which is implemented in the RowWithTuple class

			L1 = new ArrayList<RowWithTuple>();

			// we don't need to store tuples in this array
			L2 = new ArrayList<Row>();

			// this variable is used to keep track of the record id of the current tuple
			RID rid = new RID();

			// this variable is used to keep track of the field
			// to be sorted of the current tuple being read
			int field_to_sort;

			// perm is used to project the read tuples from the heapfile
			// on the field to be selected to avoid unnecessary data being stored in the
			// array
			FldSpec perm[];
			perm = new FldSpec[1];
			perm[0] = perm_mat[0];

			while (true) {
				if (!this.getNextHeapFile(len_in1, _in1, relationName)) {

					hf = new Heapfile(relationName);
					inner = hf.openScan();

					try {
						// read tuples from the heap file and fill the L1 array and L2 array
						while ((inner_tuple = inner.getNext(rid)) != null) {
							// set the header of the read tuple
							inner_tuple.setHdr((short) in1_len, _in1, t1_str_sizes);

							// set the field to be sorted
							field_to_sort = inner_tuple.getIntFld(outFilter[0].operand1.symbol.offset);

							// project the tuple on the field to be selected finally
							Projection.Project(inner_tuple, _in1, Jtuple, perm, 1);

							// add element to L1 array
							L1.add(new RowWithTuple(heapfile_index, rid, field_to_sort, Jtuple));

							// set the field to be sorted
							field_to_sort = inner_tuple.getIntFld(outFilter[1].operand1.symbol.offset);

							// add element to L2 array
							L2.add(new Row(heapfile_index, rid, field_to_sort));

							// keep track of the number of rows
							i += 1;

						}

					} catch (Exception e) {
						System.err.println("" + e);
						e.printStackTrace();
						Runtime.getRuntime().exit(1);
					}

					Heapfile F = new Heapfile(relationName);
					F.deleteFile();
				} else {

					Tuple t = new Tuple();
					t = null;
					hf = new Heapfile(relationName);
					inner = hf.openScan();
					try {
						// read tuples from the heap file and fill the L1 array and L2 array
						while ((inner_tuple = inner.getNext(rid)) != null) {

							// set the header of the read tuple
							inner_tuple.setHdr((short) in1_len, _in1, t1_str_sizes);

							// set the field to be sorted
							field_to_sort = inner_tuple.getIntFld(outFilter[0].operand1.symbol.offset);

							// project the tuple on the field to be selected finally
							Projection.Project(inner_tuple, _in1, Jtuple, perm, 1);

							// add element to L1 array
							L1.add(new RowWithTuple(heapfile_index, rid, field_to_sort, Jtuple));
							field_to_sort = inner_tuple.getIntFld(outFilter[1].operand1.symbol.offset);
							
							// add element to L2 array
							L2.add(new Row(heapfile_index, rid, field_to_sort));

							// keep track of the number of rows
							i += 1;

						}

					} catch (Exception e) {
						System.err.println("" + e);
						e.printStackTrace();
						Runtime.getRuntime().exit(1);
					}

					Heapfile F = new Heapfile(relationName);
					F.deleteFile();
					break;
				}
				heapfile_index++;
			}

			number_of_rows = i;

			// sort L1 and L2 array according to sort order

			if (Ascending_op1) {
				Collections.sort(L1, new SortAsceding());
			}

			else {
				Collections.sort(L1, new SortDesceding());
			}

			if (Descending_op2) {
				Collections.sort(L2, new SortDesceding());
			}

			else {
				Collections.sort(L2, new SortAsceding());
			}

			// initialize bit array
			BitArray = new BitSet(number_of_rows);

			// initialize permutation arrays
			permutation_array = new int[number_of_rows];

			// this permutation array is used to retrieve fields
			// of specific elements and compare them before returning the joined
			// tuple
			permutation_array_1_to_2 = new int[number_of_rows];

			// filling the permutation arrays
			for (i = 0; i < number_of_rows; i++)
				for (int j = 0; j < number_of_rows; j++) {
					if (L2.get(i).equals(L1.get(j))) {
						permutation_array[i] = j;
						break;
					}
				}

			for (i = 0; i < number_of_rows; i++)
				for (int j = 0; j < number_of_rows; j++) {
					if (L1.get(i).equals(L2.get(j))) {
						permutation_array_1_to_2[i] = j;
						break;
					}
				}

		}

		catch (Exception e) {
			System.err.println("*** Error preparing for sorting");
			System.err.println("" + e);
			Runtime.getRuntime().exit(1);
		}

		// check if or equal or not
		if ((outFilter[0].op.attrOperator == AttrOperator.aopGE || outFilter[0].op.attrOperator == AttrOperator.aopLE)
				&& (outFilter[1].op.attrOperator == AttrOperator.aopGE
						|| outFilter[1].op.attrOperator == AttrOperator.aopLE)) {

			eqoff = 0;
		}

		else {

			eqoff = 1;
		}

	}

	/**
	 * @return The joined tuple is returned
	 * @exception IOException               I/O errors
	 * @exception JoinsException            some join exception
	 * @exception IndexException            exception from super class
	 * @exception InvalidTupleSizeException invalid tuple size
	 * @exception InvalidTypeException      tuple type not valid
	 * @exception PageNotReadException      exception from lower layer
	 * @exception TupleUtilsException       exception from using tuple utilities
	 * @exception PredEvalException         exception from PredEval class
	 * @exception SortException             sort exception
	 * @exception LowMemException           memory error
	 * @exception UnknowAttrType            attribute type unknown
	 * @exception UnknownKeyTypeException   key type unknown
	 * @exception Exception                 other exceptions
	 * 
	 */
	public Tuple get_next() throws IOException, JoinsException, IndexException, InvalidTupleSizeException,
			InvalidTypeException, PageNotReadException, TupleUtilsException, PredEvalException, SortException,
			LowMemException, UnknowAttrType, UnknownKeyTypeException, Exception {
		int outer_tuple_fld1, outer_tuple_fld2, inner_tuple_fld1, inner_tuple_fld2;

		while (outer_index < number_of_rows) {

			if (get_from_outer) {
				// first parse of this outer index
				// so we need to set inner index and current bit array position
				pos = permutation_array[outer_index];
				BitArray.set(pos);
				inner_index = pos + eqoff;
				get_from_outer = false;
			}

			while (inner_index < number_of_rows) {
				if (BitArray.get(inner_index)) {
					// get outer and inner tuple
					outer_tuple = L1.get(inner_index).field_to_select;
					inner_tuple = L1.get(pos).field_to_select;

					// retrieve fields to compare them to avoid duplicates being returned
					outer_tuple_fld1 = L1.get(inner_index).field_to_sort;
					outer_tuple_fld2 = L2.get(permutation_array_1_to_2[inner_index]).field_to_sort;
					inner_tuple_fld1 = L1.get(pos).field_to_sort;
					inner_tuple_fld2 = L2.get(permutation_array_1_to_2[pos]).field_to_sort;

					// this function is used here to deal with duplicates it checks if the
					// 2 tuples satisfy the join predicate
					if (Utils.predicate_evaluate(outer_tuple_fld1, inner_tuple_fld1,
							OutputFilter[0].op.attrOperator)
							&& Utils.predicate_evaluate(outer_tuple_fld2, inner_tuple_fld2,
									OutputFilter[1].op.attrOperator)) {

						Projection.Join(outer_tuple, _in1, inner_tuple, _in2, Jtuple, perm_mat, nOutFlds);
						inner_index++;
						return Jtuple;
					}
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
	 * implement the abstract method close() from super class Iterator to finish
	 * cleaning up
	 * 
	 * @exception IOException    I/O error from lower layers
	 * @exception JoinsException join error from lower layers
	 * @exception IndexException index access error
	 */
	public void close() throws JoinsException, IOException, IndexException {
		if (!closeFlag) {

			try {
				outer.close();
				inner.closescan();
			} catch (Exception e) {
				throw new JoinsException(e, "NestedLoopsJoin.java: error in closing iterator.");
			}
			closeFlag = true;
		}
	}
}
