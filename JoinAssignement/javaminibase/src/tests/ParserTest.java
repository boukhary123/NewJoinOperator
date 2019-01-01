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

class Row_to_compare {

	public int fld1, fld2; // first and second field of the returned fields of the query

	public Row_to_compare(int fld1, int fld2) {
		this.fld1 = fld1;
		this.fld2 = fld2;
	}
}

class Sortasceding implements Comparator<Row_to_compare> {
	// Used for sorting in ascending order of
	// fld1
	public int compare(Row_to_compare a, Row_to_compare b) {
		int comp = a.fld1 - b.fld1;

		if (comp == 0)
			return a.fld2 - b.fld2;
		;

		return comp;

	}
}

public class ParserTest implements GlobalConst {

	public static ArrayList<Row_to_compare> L_nlj, L_ieqjoin, L_selfjoin_one, L_selfjoin_two, L_selfjoin_two_optimized,
			L_ieqjoin_optimized;

//	nested-loop join implementation
	public ParserTest(String path) {
		// initialize the number of output tuples
		int row_count = 0;

		// initialize the array of results for later comparison
		L_nlj = new ArrayList<Row_to_compare>();

		// setup system parameters
		String dbpath = "/tmp/" + System.getProperty("user.name") + ".minibase.jointestdb";
		String logpath = "/tmp/" + System.getProperty("user.name") + ".joinlog";

		String remove_cmd = "/bin/rm -rf ";
		String remove_logcmd = remove_cmd + logpath;
		String remove_dbcmd = remove_cmd + dbpath;
		String remove_joincmd = remove_cmd + dbpath;

		try {
			Runtime.getRuntime().exec(remove_logcmd);
			Runtime.getRuntime().exec(remove_dbcmd);
			Runtime.getRuntime().exec(remove_joincmd);
		} catch (IOException e) {
			System.err.println("" + e);
		}

		/*
		 * ExtendedSystemDefs extSysDef = new ExtendedSystemDefs(
		 * "/tmp/minibase.jointestdb", "/tmp/joinlog", 1000,500,200,"Clock");
		 */

		SystemDefs sysdef = new SystemDefs(dbpath, 1000, NUMBUF, "Clock");

		// load the query file
		File query_file = new File(path);

		// parse the query
		QueryParser q = new QueryParser(query_file);

		// variable indicating the end of the outer relation records
		boolean done_inner = false;

		// variable indicating the end of the inner relation records
		boolean done_outer = false;

		// heap files for the outer relation
		Heapfile R1_hf = null;

		// heap files for the inner relation
		Heapfile R2_hf = null;

		// load the outer relation file
		File rel_file1 = new File("../../" + q.relations.get(0) + ".txt");

		// load the inner relation file
		File rel_file2 = new File(
				"../../" + ((q.relations.size() > 1) ? q.relations.get(1) : q.relations.get(0)) + ".txt");

		try {

			// outer relation file reader
			BufferedReader rel_reader1 = new BufferedReader(new FileReader(rel_file1));

			// inner relation file reader
			BufferedReader rel_reader2;

			// record to be read for the relation files
			String rec1;
			String rec2;

			// read header of outer relation
			rec1 = rel_reader1.readLine();

			// build up too 400 outer relation heap files sequentially
			// in such a way that each heap file fits in memory
			for (int i = 0; i < 400 && done_outer == false; i++) {

				done_inner = false;

				int R_count;

				// setup the tuple for heap file building
				Tuple t = new Tuple();
				try {
					t.setHdr((short) q.R1_no_flds, q.R1types, null);
				} catch (Exception e) {
					System.err.println("*** error in Tuple.setHdr() ***");
					e.printStackTrace();
				}

				int size = t.size();

				// inserting the tuple into the heap file "R1.in" (outer relation)
				RID rid;
				try {
					R1_hf = new Heapfile("R1.in");
				} catch (Exception e) {
					System.err.println("*** error in Heapfile constructor ***");
					e.printStackTrace();
				}

				t = new Tuple(size);
				try {
					t.setHdr((short) q.R1_no_flds, q.R1types, null);
				} catch (Exception e) {
					System.err.println("*** error in Tuple.setHdr() ***");
					e.printStackTrace();
				}

				try {
					R_count = 0;

					// insert up to 5000 records into the heap file
					while (R_count < 5000) {

						// test if the relation file is ended
						if ((rec1 = rel_reader1.readLine()) == null) {
							done_outer = true;
							break;
						}

						// read each field for each tuple
						List<String> fields = Arrays.asList(rec1.split(","));

						for (int k = 0; k < q.R1_no_flds; k++) {
							t.setIntFld(k + 1, Integer.parseInt(fields.get(k)));

						}

						try {
							// insert the tuple into the heap file
							rid = R1_hf.insertRecord(t.returnTupleByteArray());
							R_count++;
						} catch (Exception e) {
							System.err.println("*** error in Heapfile.insertRecord() ***");

							e.printStackTrace();
						}
					}

				} catch (Exception e) {
					System.err.println("*** Heapfile error in Tuple.setStrFld() ***");
					e.printStackTrace();
				}

				// start a reader for the outer relation
				rel_reader2 = new BufferedReader(new FileReader(rel_file2));

				// read the header of the file
				rec2 = rel_reader2.readLine();

				// build up too 400 inner relation heap files sequentially
				// in such a way that each heap file fits in memory
				for (int j = 0; j < 400 && done_inner == false; j++) {

					t = new Tuple();
					try {
						t.setHdr((short) q.R2_no_flds, q.R2types, null);
					} catch (Exception e) {
						System.err.println("*** error in Tuple.setHdr() ***");
						e.printStackTrace();
					}

					size = t.size();

					// inserting the tuple into the heap file "R2.in" (inner relation)

					try {
						R2_hf = new Heapfile("R2.in");
					} catch (Exception e) {
						System.err.println("*** error in Heapfile constructor ***");
						e.printStackTrace();
					}

					t = new Tuple(size);
					try {
						t.setHdr((short) q.R2_no_flds, q.R2types, null);
					} catch (Exception e) {
						System.err.println("*** error in Tuple.setHdr() ***");
						e.printStackTrace();
					}

					try {
						R_count = 0;

						// insert up to 5000 records into the heap file
						while (R_count < 5000) {

							if ((rec2 = rel_reader2.readLine()) == null) {
								done_inner = true;
								break;
							}

							// read each field for each tuple
							List<String> fields = Arrays.asList(rec2.split(","));

							for (int k = 0; k < q.R2_no_flds; k++) {

								t.setIntFld(k + 1, Integer.parseInt(fields.get(k)));

							}

							try {
								// insert the tuple into the heap file
								rid = R2_hf.insertRecord(t.returnTupleByteArray());
								R_count++;
							} catch (Exception e) {
								System.err.println("*** error in Heapfile.insertRecord() ***");

								e.printStackTrace();
							}
						}

					} catch (Exception e) {
						System.err.println("*** Heapfile error in Tuple.setStrFld() ***");
						e.printStackTrace();
					}

					// setup a file scan for the outer relation
					FileScan am = null;
					try {
						am = new FileScan("R1.in", q.R1types, null, (short) q.R1_no_flds, q.R1_no_flds, q.R1_projection,
								null);
					}

					catch (Exception e) {
						System.err.println("*** Error creating scan for Index scan");
						System.err.println("" + e);
						Runtime.getRuntime().exit(1);
					}

					NestedLoopsJoins nlj = null;

					// setup the nested-loop join between the two relations
					try {
						nlj = new NestedLoopsJoins(q.R1types, q.R1_no_flds, null, q.R2types, q.R2_no_flds, null, 10, am,
								"R2.in", q.Predicate, null, q.q_projection, 2);
					}

					catch (Exception e) {
						System.err.println("*** Error preparing for nested_loop_join");
						System.err.println("" + e);
						e.printStackTrace();
						Runtime.getRuntime().exit(1);
					}

					t = new Tuple();
					t = null;

					// extract output tuples one at a time
					try {
						while ((t = nlj.get_next()) != null) {
//				  						    	  t.print(q.projectionTypes);
							row_count++;
							L_nlj.add(new Row_to_compare(t.getIntFld(1), t.getIntFld(2)));

						}

					} catch (Exception e) {
						System.err.println("" + e);
						e.printStackTrace();
						Runtime.getRuntime().exit(1);
					}

					// close the iterators
					am.close();
					nlj.close();

					// delete the inner relation heap file to remove the added records
					R2_hf.deleteFile();
				}

				// delete the outer relation heap file to remove the added records
				R1_hf.deleteFile();
			}

			// print the row_count of the output
			System.out.println(row_count);
			Collections.sort(L_nlj, new Sortasceding());

		} catch (Exception e) {
			System.err.println("" + e);
		}

	}

	public static void compare_2_arrays(ArrayList<Row_to_compare> L1, ArrayList<Row_to_compare> L2) {

		int i = 0;
		boolean results_same = true;
		// if (L1.size()==L2.size()) {
		if (true) {
			System.out.println("Both arrays are of same size");

			for (i = 0; i < L2.size(); i++) {
				if (L2.get(i).fld1 == L2.get(i).fld1 && L1.get(i).fld2 == L2.get(i).fld2) {
					results_same = true;
				} else {
					System.out.println("results are different at index " + i);
					System.out.println(L2.get(i).fld1 + " " + L2.get(i).fld2);
					System.out.println(L1.get(i).fld1 + " " + L1.get(i).fld2);
					break;

				}
			}
		} else {
			System.out.println("Results have different size");
			Runtime.getRuntime().exit(1);
		}

		if (results_same) {
			System.out.println("Identical Results");
		} else
			System.out.println("Results are different");

	}

	public static void ParserTest_self_join_one() {
		L_selfjoin_one = new ArrayList<Row_to_compare>();

		int row_count = 0;
		String dbpath = "/tmp/" + System.getProperty("user.name") + ".minibase.jointestdb";
		String logpath = "/tmp/" + System.getProperty("user.name") + ".joinlog";
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
			System.err.println("" + e);
		}

		SystemDefs sysdef = new SystemDefs(dbpath, 1000, NUMBUF, "Clock");

		File query_file = new File("../../query_2a.txt");
		QueryParser q = new QueryParser(query_file);

		String relation = q.relations.get(0);

		File rel_file = new File("../../" + relation + ".txt");

		SelfJoinOnePredicate nlj = null;
		try {
			nlj = new SelfJoinOnePredicate(q.R1types, q.R1_no_flds, null, q.R2types, q.R2_no_flds, null, 10, null,
					"R.in", q.Predicate, null, q.q_projection, 2, q.relations.get(0));
		}

		catch (Exception e) {
			System.err.println("*** Error preparing for nested_loop_join");
			System.err.println("" + e);
			e.printStackTrace();
			Runtime.getRuntime().exit(1);
		}

		Tuple t = new Tuple();
		t = null;
		try {
			while ((t = nlj.get_next()) != null) {
				// t.print(q.projectionTypes);
				row_count++;
				L_selfjoin_one.add(new Row_to_compare(t.getIntFld(1), t.getIntFld(2)));
			}
		} catch (Exception e) {
			System.err.println("" + e);
			e.printStackTrace();
			Runtime.getRuntime().exit(1);
		}

		System.out.println(row_count);
		Collections.sort(L_selfjoin_one, new Sortasceding());
	}

	public static void ParserTest_self_join_two() {
		L_selfjoin_two = new ArrayList<Row_to_compare>();

		int row_count = 0;
		String dbpath = "/tmp/" + System.getProperty("user.name") + ".minibase.jointestdb";
		String logpath = "/tmp/" + System.getProperty("user.name") + ".joinlog";
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
			System.err.println("" + e);
		}

		SystemDefs sysdef = new SystemDefs(dbpath, 1000, NUMBUF, "Clock");

		File query_file = new File("../../query_2b.txt");
		QueryParser q = new QueryParser(query_file);

		String relation = q.relations.get(0);

		File rel_file = new File("../../" + relation + ".txt");

		SelfInequalityJoinTwoPredicate nlj = null;
		try {
			nlj = new SelfInequalityJoinTwoPredicate(q.R1types, q.R1_no_flds, null, q.R2types, q.R2_no_flds, null, 10,
					null, "R.in", q.Predicate, null, q.q_projection, 2, q.relations.get(0));
		}

		catch (Exception e) {
			System.err.println("*** Error preparing for nested_loop_join");
			System.err.println("" + e);
			e.printStackTrace();
			Runtime.getRuntime().exit(1);
		}

		Tuple t = new Tuple();
		t = null;
		try {
			while ((t = nlj.get_next()) != null) {
				// t.print(q.projectionTypes);
				row_count++;
				L_selfjoin_two.add(new Row_to_compare(t.getIntFld(1), t.getIntFld(2)));
			}
		} catch (Exception e) {
			System.err.println("" + e);
			e.printStackTrace();
			Runtime.getRuntime().exit(1);
		}

		System.out.println(row_count);
		Collections.sort(L_selfjoin_two, new Sortasceding());
	}

	public static void ParserTest_ie_join() {
		L_ieqjoin = new ArrayList<Row_to_compare>();

		int row_count = 0;
		String dbpath = "/tmp/" + System.getProperty("user.name") + ".minibase.jointestdb";
		String logpath = "/tmp/" + System.getProperty("user.name") + ".joinlog";
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
			System.err.println("" + e);
		}

		SystemDefs sysdef = new SystemDefs(dbpath, 1000, NUMBUF, "Clock");

		File query_file = new File("../../query_2c.txt");
		QueryParser q = new QueryParser(query_file);

		String relation = q.relations.get(0);

		File rel_file = new File("../../" + relation + ".txt");

		InequalityJoinTwoPredicates nlj = null;
		try {
			nlj = new InequalityJoinTwoPredicates(q.R1types, q.R1_no_flds, null, q.R2types, q.R2_no_flds, null, 10,
					null, "R1.in", "R2.in", q.Predicate, null, q.q_projection, 2, q.relations.get(0),
					q.relations.get(1));
		}

		catch (Exception e) {
			System.err.println("*** Error preparing for nested_loop_join");
			System.err.println("" + e);
			e.printStackTrace();
			Runtime.getRuntime().exit(1);
		}

		Tuple t = new Tuple();
		t = null;
		try {
			while ((t = nlj.get_next()) != null) {
				// t.print(q.projectionTypes);
				row_count++;
				L_ieqjoin.add(new Row_to_compare(t.getIntFld(1), t.getIntFld(2)));
			}
		} catch (Exception e) {
			System.err.println("" + e);
			e.printStackTrace();
			Runtime.getRuntime().exit(1);
		}

		System.out.println(row_count);
		Collections.sort(L_ieqjoin, new Sortasceding());
	}

	public static void ParserTest_self_join_two_optimized() {
		L_selfjoin_two_optimized = new ArrayList<Row_to_compare>();

		int row_count = 0;
		String dbpath = "/tmp/" + System.getProperty("user.name") + ".minibase.jointestdb";
		String logpath = "/tmp/" + System.getProperty("user.name") + ".joinlog";
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
			System.err.println("" + e);
		}

		SystemDefs sysdef = new SystemDefs(dbpath, 1000, NUMBUF, "Clock");

		File query_file = new File("../../query_2b.txt");
		QueryParser q = new QueryParser(query_file);

		String relation = q.relations.get(0);

		File rel_file = new File("../../" + relation + ".txt");

		SelfInequalityJoinTwoPredicateOptimized nlj = null;
		try {
			nlj = new SelfInequalityJoinTwoPredicateOptimized(q.R1types, q.R1_no_flds, null, q.R2types, q.R2_no_flds,
					null, 10, null, "R.in", q.Predicate, null, q.q_projection, 2, 10, q.relations.get(0));
		}

		catch (Exception e) {
			System.err.println("*** Error preparing for nested_loop_join");
			System.err.println("" + e);
			e.printStackTrace();
			Runtime.getRuntime().exit(1);
		}

		Tuple t = new Tuple();
		t = null;
		try {
			while ((t = nlj.get_next()) != null) {
				// t.print(q.projectionTypes);
				row_count++;
				L_selfjoin_two_optimized.add(new Row_to_compare(t.getIntFld(1), t.getIntFld(2)));
			}
		} catch (Exception e) {
			System.err.println("" + e);
			e.printStackTrace();
			Runtime.getRuntime().exit(1);
		}

		System.out.println(row_count);
		Collections.sort(L_selfjoin_two_optimized, new Sortasceding());
	}

	public static void ParserTest_ie_join_optimized() {
		L_ieqjoin_optimized = new ArrayList<Row_to_compare>();

		int row_count = 0;
		String dbpath = "/tmp/" + System.getProperty("user.name") + ".minibase.jointestdb";
		String logpath = "/tmp/" + System.getProperty("user.name") + ".joinlog";
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
			System.err.println("" + e);
		}

		SystemDefs sysdef = new SystemDefs(dbpath, 1000, NUMBUF, "Clock");

		File query_file = new File("../../query_2c_2.txt");
		QueryParser q = new QueryParser(query_file);

		String relation = q.relations.get(0);

		File rel_file = new File("../../" + relation + ".txt");

		InequalityJoinTwoPredicatesOptimized nlj = null;
		try {
			nlj = new InequalityJoinTwoPredicatesOptimized(q.R1types, q.R1_no_flds, null, q.R2types, q.R2_no_flds, null,
					10, null, "R1.in", "R2.in", q.Predicate, null, q.q_projection, 2, 10, q.relations.get(0),
					q.relations.get(1));
		}

		catch (Exception e) {
			System.err.println("*** Error preparing for nested_loop_join");
			System.err.println("" + e);
			e.printStackTrace();
			Runtime.getRuntime().exit(1);
		}

		Tuple t = new Tuple();
		t = null;
		try {
			while ((t = nlj.get_next()) != null) {
				// t.print(q.projectionTypes);
				row_count++;
				L_ieqjoin_optimized.add(new Row_to_compare(t.getIntFld(1), t.getIntFld(2)));
			}
		} catch (Exception e) {
			System.err.println("" + e);
			e.printStackTrace();
			Runtime.getRuntime().exit(1);
		}

		System.out.println(row_count);
		Collections.sort(L_ieqjoin_optimized, new Sortasceding());
	}

	public static void main(String argv[]) {
		long start = System.currentTimeMillis();
		ParserTest test = new ParserTest("../../query_2c_2.txt");
		long end = System.currentTimeMillis();

		System.out.println("NLJ takes " + (end - start) + "ms");
		start = System.currentTimeMillis();
		ParserTest_ie_join_optimized();
		end = System.currentTimeMillis();
		System.out.println("IESelfJoin takes " + (end - start) + "ms");

		// compare_2_arrays(L_nlj, L_ieqjoin);
	}
}
