package tests;

import java.io.*;

import org.jfree.chart.ChartFactory;
import org.jfree.chart.JFreeChart;
import org.jfree.data.xy.XYSeries;
import org.jfree.chart.plot.PlotOrientation;
import org.jfree.data.xy.XYSeriesCollection;

import iterator.SelfInequalityJoinTwoPredicate;
import iterator.SelfInequalityJoinTwoPredicateOptimized;
import iterator.SelfJoinOnePredicate;

import org.jfree.chart.ChartUtilities; 

public class plot2 {

   public static void main( String[ ] args )throws Exception {
	   		
    	  Process process = Runtime.getRuntime().exec("../../../../Report/extract.sh 1001");
    	  process.waitFor();
    	  
    	  System.out.println("For 1000 input tuples");
    	  
//    	  // NLJ test
//    	  
//		  long start = System.currentTimeMillis();
//		  ParserTestNlj test1 = new ParserTestNlj("../../../../Output/query_2b.txt","../../../../Output/Joined_Result_Query_2b_nlj.txt");
//		  long end = System.currentTimeMillis();
//		  
//		  long runtime = end - start;
//		  System.out.println("NLJ Runtime:" + runtime + " ms");
//		  
//	      final XYSeries nlj = new XYSeries( "Nested Loop Join" );
//	      nlj.add( 1000 , runtime );
	      
	      // IE join test
	      
	      long start = System.currentTimeMillis();
	      ParserTestIEJoin<SelfInequalityJoinTwoPredicate> test2 = new ParserTestIEJoin<SelfInequalityJoinTwoPredicate>("../../../../Output/query_2b.txt",
					"../../../../Output/Joined_Result_Query_2b_ie.txt","SelfInequalityJoinTwoPredicate");
	      long end = System.currentTimeMillis();
	      
		  long runtime = end - start;
		  System.out.println("IE Runtime:" + runtime + " ms");
	      
	      final XYSeries iej = new XYSeries( "Inequality Join" );
	      iej.add( 1000 , runtime );
	      
	      // IE join optimized test 10 chunks
	      
	      start = System.currentTimeMillis();
		  ParserTestIEJoin<SelfInequalityJoinTwoPredicateOptimized> test3 = new ParserTestIEJoin<SelfInequalityJoinTwoPredicateOptimized>("../../../../Output/query_2b.txt",
					"../../../../Output/Joined_Result_Query_2d_2b.txt","SelfInequalityJoinTwoPredicateOptimized", 10);
	      end = System.currentTimeMillis();
	      
		  runtime = end - start;
		  System.out.println("IE Optimized (10 Chunks) Runtime:" + runtime + " ms");
	      
	      final XYSeries iej_op_1 = new XYSeries( "Inequality Join Optimized (10 Chunks)" );
	      iej_op_1.add( 1000 , runtime );
	      
	      // IE join optimized test 100 chunks
	      
	      start = System.currentTimeMillis();
		  ParserTestIEJoin<SelfInequalityJoinTwoPredicateOptimized> test4 = new ParserTestIEJoin<SelfInequalityJoinTwoPredicateOptimized>("../../../../Output/query_2b.txt",
					"../../../../Output/Joined_Result_Query_2d_2b.txt","SelfInequalityJoinTwoPredicateOptimized", 100);
	      end = System.currentTimeMillis();
	      
		  runtime = end - start;
		  System.out.println("IE Optimized (100 Chunks) Runtime:" + runtime + " ms");
	      
	      final XYSeries iej_op_2 = new XYSeries( "Inequality Join Optimized (100 Chunks)" );
	      iej_op_2.add( 1000 , runtime );
	      
	   // IE join optimized test 1000 chunk
	      
	      start = System.currentTimeMillis();
		  ParserTestIEJoin<SelfInequalityJoinTwoPredicateOptimized> test5 = new ParserTestIEJoin<SelfInequalityJoinTwoPredicateOptimized>("../../../../Output/query_2b.txt",
					"../../../../Output/Joined_Result_Query_2d_2b.txt","SelfInequalityJoinTwoPredicateOptimized", 1000);
	      end = System.currentTimeMillis();
	      
		  runtime = end - start;
		  System.out.println("IE Optimized (1000 Chunks) Runtime:" + runtime + " ms");
	      
	      final XYSeries iej_op_3 = new XYSeries( "Inequality Join Optimized (1000 Chunk)" );
	      iej_op_3.add( 1000 , runtime );
	      
	      
	      
    	  process = Runtime.getRuntime().exec("../../../../Report/extract.sh 10001");
    	  process.waitFor();

    	  System.out.println("For 10000 input tuples");

    	  // NLJ test
    	  
//		  start = System.currentTimeMillis();
//		  ParserTestNlj test6 = new ParserTestNlj("../../../../Output/query_2b.txt","../../../../Output/Joined_Result_Query_2b_nlj.txt");
//		  end = System.currentTimeMillis();
//		  
//		  runtime = end - start;
//		  System.out.println("NLJ Runtime:" + runtime + " ms");
//		  
//	      nlj.add( 10000 , runtime );
	      
	      // IE join test
	      
	      start = System.currentTimeMillis();
	      ParserTestIEJoin<SelfInequalityJoinTwoPredicate> test7 = new ParserTestIEJoin<SelfInequalityJoinTwoPredicate>("../../../../Output/query_2b.txt",
					"../../../../Output/Joined_Result_Query_2b_ie.txt","SelfInequalityJoinTwoPredicate");
	      end = System.currentTimeMillis();
	      
		  runtime = end - start;
		  System.out.println("IE Runtime:" + runtime + " ms");
	  
	      iej.add( 10000 , runtime);
	      
	      
	      // IE join optimized test 10 chunks
	      
	      start = System.currentTimeMillis();
		  ParserTestIEJoin<SelfInequalityJoinTwoPredicateOptimized> test8 = new ParserTestIEJoin<SelfInequalityJoinTwoPredicateOptimized>("../../../../Output/query_2b.txt",
					"../../../../Output/Joined_Result_Query_2d_2b.txt","SelfInequalityJoinTwoPredicateOptimized", 10);
	      end = System.currentTimeMillis();
	      
		  runtime = end - start;
		  System.out.println("IE Optimized (10 Chunks) Runtime:" + runtime + " ms");
	      
	      iej_op_1.add( 10000 , runtime );
	      
	      // IE join optimized test 100 chunks
	      
	      start = System.currentTimeMillis();
		  ParserTestIEJoin<SelfInequalityJoinTwoPredicateOptimized> test9 = new ParserTestIEJoin<SelfInequalityJoinTwoPredicateOptimized>("../../../../Output/query_2b.txt",
					"../../../../Output/Joined_Result_Query_2d_2b.txt","SelfInequalityJoinTwoPredicateOptimized", 100);
	      end = System.currentTimeMillis();
	      
		  runtime = end - start;
		  System.out.println("IE Optimized (100 Chunks) Runtime:" + runtime + " ms");
	      
	      iej_op_2.add( 10000 , runtime );
	      
	   // IE join optimized test 1000 chunks
	      
	      start = System.currentTimeMillis();
		  ParserTestIEJoin<SelfInequalityJoinTwoPredicateOptimized> test10 = new ParserTestIEJoin<SelfInequalityJoinTwoPredicateOptimized>("../../../../Output/query_2b.txt",
					"../../../../Output/Joined_Result_Query_2d_2b.txt","SelfInequalityJoinTwoPredicateOptimized", 1000);
	      end = System.currentTimeMillis();
	      
		  runtime = end - start;
		  System.out.println("IE Optimized (1000 Chunks) Runtime:" + runtime + " ms");
	      
	      iej_op_3.add( 10000 , runtime );
	      
	      
    	  process = Runtime.getRuntime().exec("../../../../Report/extract.sh 50001");
    	  process.waitFor();

    	  System.out.println("For 50000 input tuples");

    	  // NLJ test
    	  
//		  start = System.currentTimeMillis();
//		  ParserTestNlj test11 = new ParserTestNlj("../../../../Output/query_2b.txt","../../../../Output/Joined_Result_Query_2a_nlj.txt");
//		  end = System.currentTimeMillis();
//		  
//		  runtime = end - start;
//		  System.out.println("NLJ Runtime:" + runtime + " ms");
//		  
//	      nlj.add( 50000 , runtime );
	      
	      start = System.currentTimeMillis();
	      ParserTestIEJoin<SelfInequalityJoinTwoPredicate> test12 = new ParserTestIEJoin<SelfInequalityJoinTwoPredicate>("../../../../Output/query_2b.txt",
					"../../../../Output/Joined_Result_Query_2b_ie.txt","SelfInequalityJoinTwoPredicate");
	       end = System.currentTimeMillis();
	      
		  
		  runtime = end - start;
		  System.out.println("IE Runtime:" + runtime + " ms");
	      
	      iej.add( 50000 , runtime );
	      
	      // IE join optimized test 10 chunks
	      
	      start = System.currentTimeMillis();
		  ParserTestIEJoin<SelfInequalityJoinTwoPredicateOptimized> test13 = new ParserTestIEJoin<SelfInequalityJoinTwoPredicateOptimized>("../../../../Output/query_2b.txt",
					"../../../../Output/Joined_Result_Query_2d_2b.txt","SelfInequalityJoinTwoPredicateOptimized", 10);
	      end = System.currentTimeMillis();
	      
		  runtime = end - start;
		  System.out.println("IE Optimized (10 Chunks) Runtime:" + runtime + " ms");
	      
	      iej_op_1.add( 50000 , runtime );
	      
	      // IE join optimized test 20%
	      
	      start = System.currentTimeMillis();
		  ParserTestIEJoin<SelfInequalityJoinTwoPredicateOptimized> test14 = new ParserTestIEJoin<SelfInequalityJoinTwoPredicateOptimized>("../../../../Output/query_2b.txt",
					"../../../../Output/Joined_Result_Query_2d_2b.txt","SelfInequalityJoinTwoPredicateOptimized", 100);
	      end = System.currentTimeMillis();
	      
		  runtime = end - start;
		  System.out.println("IE Optimized (100 Chunks) Runtime:" + runtime + " ms");
	      
	      iej_op_2.add( 50000 , runtime );
	      
	   // IE join optimized test 30%
	      
	      start = System.currentTimeMillis();
		  ParserTestIEJoin<SelfInequalityJoinTwoPredicateOptimized> test15 = new ParserTestIEJoin<SelfInequalityJoinTwoPredicateOptimized>("../../../../Output/query_2b.txt",
					"../../../../Output/Joined_Result_Query_2d_2b.txt","SelfInequalityJoinTwoPredicateOptimized", 1000);
	      end = System.currentTimeMillis();
	      
		  runtime = end - start;
		  System.out.println("IE Optimized (1000 Chunk) Runtime:" + runtime + " ms");
	      
	      iej_op_3.add( 50000 , runtime );
    	  
	      final XYSeriesCollection dataset = new XYSeriesCollection( );
//	      dataset.addSeries( nlj );
	      dataset.addSeries( iej );
	      dataset.addSeries( iej_op_1 );
	      dataset.addSeries( iej_op_2 );
	      dataset.addSeries( iej_op_3 );
	
	      JFreeChart xylineChart = ChartFactory.createXYLineChart(
	         "Different Self Join Algorithms Runtime with two predicates", 
	         "Input Cardinality",
	         "Runtime in ms", 
	         dataset,
	         PlotOrientation.VERTICAL, 
	         true, true, false);
	      
	      int width = 640;   /* Width of the image */
	      int height = 480;  /* Height of the image */ 
	      File XYChart = new File( "../../../../Report/XYLineChart2.jpeg" ); 
	      ChartUtilities.saveChartAsJPEG( XYChart, xylineChart, width, height);
   }
}
