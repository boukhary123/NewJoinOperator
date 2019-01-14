package tests;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.UnsupportedEncodingException;
import global.GlobalConst;
import global.SystemDefs;
import heap.Tuple;
import iterator.*;

public class ParserTestIEJoin<T extends Iterator> implements GlobalConst {

	static PrintWriter writer;

	public ParserTestIEJoin(String path_query, String path_output, String type) {

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

		File query_file = new File(path_query);
		QueryParser q = new QueryParser(query_file);

		String relation = q.relations.get(0);

		File rel_file = new File("../../../../Output/" + relation + ".txt");
		T iejoin = null;
		switch (type) {

		case "SelfJoinOnePredicate":

			try {
				iejoin = (T) new SelfJoinOnePredicate(q.R1types, q.R1_no_flds, null, q.R2types, q.R2_no_flds, null, 10,
						null, "R.in", q.Predicate, null, q.q_projection, 2, q.relations.get(0));
			}

			catch (Exception e) {
				System.err.println("*** Error preparing for Inequality Join");
				System.err.println("" + e);
				e.printStackTrace();
				Runtime.getRuntime().exit(1);
			}
			
			break;
			
		case "SelfInequalityJoinTwoPredicate": 
			try {
				iejoin = (T) new SelfInequalityJoinTwoPredicate(q.R1types, q.R1_no_flds, null, q.R2types, q.R2_no_flds,
						null, 10, null, "R.in", q.Predicate, null, q.q_projection, 2, q.relations.get(0));
			}

			catch (Exception e) {
				System.err.println("*** Error preparing for nested_loop_join");
				System.err.println("" + e);
				e.printStackTrace();
				Runtime.getRuntime().exit(1);
			}
			
			break;


		case "InequalityJoinTwoPredicates": 

			try {
				iejoin = (T) new InequalityJoinTwoPredicates(q.R1types, q.R1_no_flds, null, q.R2types, q.R2_no_flds,
						null, 10, null, "R1.in", "R2.in", q.Predicate, null, q.q_projection, 2, q.relations.get(0),
						q.relations.get(1));
			}

			catch (Exception e) {
				System.err.println("*** Error preparing for nested_loop_join");
				System.err.println("" + e);
				e.printStackTrace();
				Runtime.getRuntime().exit(1);
			}
			break;
			
		case "SelfInequalityJoinTwoPredicateOptimized":

			try {
				iejoin = (T) new SelfInequalityJoinTwoPredicateOptimized(q.R1types, q.R1_no_flds, null, q.R2types,
						q.R2_no_flds, null, 10, null, "R.in", q.Predicate, null, q.q_projection, 2, 10,
						q.relations.get(0));
			}

			catch (Exception e) {
				System.err.println("*** Error preparing for nested_loop_join");
				System.err.println("" + e);
				e.printStackTrace();
				Runtime.getRuntime().exit(1);
			}
			
			break;


		default: 
			try {
				iejoin = (T) new InequalityJoinTwoPredicatesOptimized(q.R1types, q.R1_no_flds, null, q.R2types,
						q.R2_no_flds, null, 10, null, "R1.in", "R2.in", q.Predicate, null, q.q_projection, 2, 10,
						q.relations.get(0), q.relations.get(1));
			}

			catch (Exception e) {
				System.err.println("*** Error preparing for nested_loop_join");
				System.err.println("" + e);
				e.printStackTrace();
				Runtime.getRuntime().exit(1);
			}
			
			break;

		}

		extract_results(iejoin);

	}

	public ParserTestIEJoin(String path_query, String path_output, String type, int number_of_chunks) {

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

		File query_file = new File(path_query);
		QueryParser q = new QueryParser(query_file);

		String relation = q.relations.get(0);

		File rel_file = new File("../../../../Output/" + relation + ".txt");
		T iejoin = null;
		switch (type) {

		case "SelfJoinOnePredicate":

			try {
				iejoin = (T) new SelfJoinOnePredicate(q.R1types, q.R1_no_flds, null, q.R2types, q.R2_no_flds, null, 10,
						null, "R.in", q.Predicate, null, q.q_projection, 2, q.relations.get(0));
			}

			catch (Exception e) {
				System.err.println("*** Error preparing for Inequality Join");
				System.err.println("" + e);
				e.printStackTrace();
				Runtime.getRuntime().exit(1);
			}
			
			break;
			
		case "SelfInequalityJoinTwoPredicate": 
			try {
				iejoin = (T) new SelfInequalityJoinTwoPredicate(q.R1types, q.R1_no_flds, null, q.R2types, q.R2_no_flds,
						null, 10, null, "R.in", q.Predicate, null, q.q_projection, 2, q.relations.get(0));
			}

			catch (Exception e) {
				System.err.println("*** Error preparing for nested_loop_join");
				System.err.println("" + e);
				e.printStackTrace();
				Runtime.getRuntime().exit(1);
			}
			
			break;


		case "InequalityJoinTwoPredicates": 

			try {
				iejoin = (T) new InequalityJoinTwoPredicates(q.R1types, q.R1_no_flds, null, q.R2types, q.R2_no_flds,
						null, 10, null, "R1.in", "R2.in", q.Predicate, null, q.q_projection, 2, q.relations.get(0),
						q.relations.get(1));
			}

			catch (Exception e) {
				System.err.println("*** Error preparing for nested_loop_join");
				System.err.println("" + e);
				e.printStackTrace();
				Runtime.getRuntime().exit(1);
			}
			break;
			
		case "SelfInequalityJoinTwoPredicateOptimized":

			try {
				iejoin = (T) new SelfInequalityJoinTwoPredicateOptimized(q.R1types, q.R1_no_flds, null, q.R2types,
						q.R2_no_flds, null, 10, null, "R.in", q.Predicate, null, q.q_projection, 2, number_of_chunks,
						q.relations.get(0));
			}

			catch (Exception e) {
				System.err.println("*** Error preparing for nested_loop_join");
				System.err.println("" + e);
				e.printStackTrace();
				Runtime.getRuntime().exit(1);
			}
			
			break;


		default: 
			try {
				iejoin = (T) new InequalityJoinTwoPredicatesOptimized(q.R1types, q.R1_no_flds, null, q.R2types,
						q.R2_no_flds, null, 10, null, "R1.in", "R2.in", q.Predicate, null, q.q_projection, 2, number_of_chunks,
						q.relations.get(0), q.relations.get(1));
			}

			catch (Exception e) {
				System.err.println("*** Error preparing for nested_loop_join");
				System.err.println("" + e);
				e.printStackTrace();
				Runtime.getRuntime().exit(1);
			}
			
			break;

		}

		extract_results(iejoin);

	}
	
	private void extract_results(T nlj) {

		int row_count = 0;
		Tuple t = new Tuple();
		t = null;
		try {
			while ((t = nlj.get_next()) != null) {
				writer.println(String.format("%d,%d", t.getIntFld(1), t.getIntFld(2)));
				row_count++;
			}
		} catch (Exception e) {
			System.err.println("" + e);
			e.printStackTrace();
			Runtime.getRuntime().exit(1);
		}

		System.out.println(String.format("The number of rows is %d", row_count));
	}
}
