package tests;

import java.io.*;

import org.jfree.chart.ChartFactory;
import org.jfree.chart.JFreeChart;
import org.jfree.data.xy.XYSeries;
import org.jfree.chart.plot.PlotOrientation;
import org.jfree.data.xy.XYSeriesCollection;

import iterator.SelfInequalityJoinTwoPredicate;
import iterator.SelfJoinOnePredicate;

import org.jfree.chart.ChartUtilities; 

public class plot {

   public static void main( String[ ] args )throws Exception {
	   		
    	  Process process = Runtime.getRuntime().exec("../../../../Report/extract.sh 1001");
       
    	  System.out.println("For 1000 input tuples");
    	  
		  long start = System.currentTimeMillis();
		  ParserTestNlj test1 = new ParserTestNlj("../../../../Output/query_2a.txt","../../../../Output/Joined_Result_Query_2a_nlj.txt");
		  long end = System.currentTimeMillis();
		  
		  long runtime = end - start;
		  System.out.println("NLJ Runtime:" + runtime + " ms");
		  
	      final XYSeries nlj = new XYSeries( "Nested Loop Join" );
	      nlj.add( 1000 , runtime );
	      
	      
	      
	      start = System.currentTimeMillis();
	      ParserTestIEJoin<SelfJoinOnePredicate> test2 = new ParserTestIEJoin<SelfJoinOnePredicate>("../../../../Output/query_2a.txt",
					"../../../../Output/Joined_Result_Query_2a_iej.txt","SelfJoinOnePredicate");
	      end = System.currentTimeMillis();
	      
		  runtime = end - start;
		  System.out.println("IE Runtime:" + runtime + " ms");
	      
	      final XYSeries iej = new XYSeries( "Inequality Join" );
	      iej.add( 1000 , runtime );
	      
	      
	      
    	  process = Runtime.getRuntime().exec("../../../../Report/extract.sh 10001");

    	  System.out.println("For 10000 input tuples");

    	  
		  start = System.currentTimeMillis();
		  ParserTestNlj test3 = new ParserTestNlj("../../../../Output/query_2a.txt","../../../../Output/Joined_Result_Query_2a_nlj.txt");
		  end = System.currentTimeMillis();
		  
		  runtime = end - start;
		  System.out.println("NLJ Runtime:" + runtime + " ms");
		  
	      nlj.add( 10000 , runtime );
	      
	      start = System.currentTimeMillis();
	      ParserTestIEJoin<SelfJoinOnePredicate> test4 = new ParserTestIEJoin<SelfJoinOnePredicate>("../../../../Output/query_2a.txt",
					"../../../../Output/Joined_Result_Query_2a_iej.txt","SelfJoinOnePredicate");
	      end = System.currentTimeMillis();
	      
		  runtime = end - start;
		  System.out.println("IE Runtime:" + runtime + " ms");
	      
	      iej.add( 10000 , runtime);
	      
	      
    	  process = Runtime.getRuntime().exec("../../../../Report/extract.sh 50001");

    	  System.out.println("For 50000 input tuples");

    	  
		  start = System.currentTimeMillis();
		  ParserTestNlj test5 = new ParserTestNlj("../../../../Output/query_2a.txt","../../../../Output/Joined_Result_Query_2a_nlj.txt");
		  end = System.currentTimeMillis();
		  
		  runtime = end - start;
		  System.out.println("NLJ Runtime:" + runtime + " ms");
		  
	      nlj.add( 50000 , runtime );
	      
	      start = System.currentTimeMillis();
	      ParserTestIEJoin<SelfJoinOnePredicate> test6 = new ParserTestIEJoin<SelfJoinOnePredicate>("../../../../Output/query_2a.txt",
					"../../../../Output/Joined_Result_Query_2a_iej.txt","SelfJoinOnePredicate");
	      end = System.currentTimeMillis();
	      
		  
		  runtime = end - start;
		  System.out.println("IE Runtime:" + runtime + " ms");
	      
	      iej.add( 50000 , runtime );
    	  
	      final XYSeriesCollection dataset = new XYSeriesCollection( );
	      dataset.addSeries( nlj );
	      dataset.addSeries( iej );
	
	      JFreeChart xylineChart = ChartFactory.createXYLineChart(
	         "Different Join Algorithm Runtime with single predicate", 
	         "Input Cardinality",
	         "Runtime in ms", 
	         dataset,
	         PlotOrientation.VERTICAL, 
	         true, true, false);
	      
	      int width = 640;   /* Width of the image */
	      int height = 480;  /* Height of the image */ 
	      File XYChart = new File( "../../../../Report/XYLineChart.jpeg" ); 
	      ChartUtilities.saveChartAsJPEG( XYChart, xylineChart, width, height);
   }
}
