package tests;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.UnsupportedEncodingException;
import java.util.Arrays;
import java.util.List;

import global.GlobalConst;
import global.RID;
import global.SystemDefs;
import heap.Heapfile;
import heap.Tuple;
import iterator.*;

public class ParserTestNlj implements GlobalConst {
	static PrintWriter writer;

	public ParserTestNlj(String path_query, String path_output) {
		// initialize file writer
		try {
			writer = new PrintWriter(new FileWriter(path_output));
		} catch (FileNotFoundException e) {
			System.err.println("FILENOTFOUNDEXCEPTION: " + e.getMessage());

		} catch (UnsupportedEncodingException e) {
			System.err.println("Caught UnsupportedEncodingException: " + e.getMessage());
		} catch (IOException e) {
			System.err.println("Caught IOException: " + e.getMessage());
		}

		// initialize the number of output tuples
		int row_count = 0;

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
		File query_file = new File(path_query);

		// parse the query
		QueryParser q = new QueryParser(query_file);
		
		System.out.println(q.R1_no_flds);

		// variable indicating the end of the outer relation records
		boolean done_inner = false;

		// variable indicating the end of the inner relation records
		boolean done_outer = false;

		// heap files for the outer relation
		Heapfile R1_hf = null;

		// heap files for the inner relation
		Heapfile R2_hf = null;

		// load the outer relation file
		File rel_file1 = new File("../../../" + q.relations.get(0) + ".txt");

		// load the inner relation file
		File rel_file2 = new File(
				"../../../" + ((q.relations.size() > 1) ? q.relations.get(1) : q.relations.get(0)) + ".txt");

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
							// t.print(q.projectionTypes);
							writer.println(String.format("%d,%d", t.getIntFld(1), t.getIntFld(2)));
							row_count++;

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
			System.out.println(String.format("The number of tuples of"
					+ " the joined results is %d", row_count));

		} catch (Exception e) {
			System.err.println("" + e);
		}

	}

}
