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

public class plot3 {

   public static void main( String[ ] args )throws Exception {
	   		
    	  Process process = Runtime.getRuntime().exec("../../../../Report/extract.sh 50001");
    	  process.waitFor();
    	  
    	  System.out.println("For 50000 input tuples");
    	  
    	  long start, end, runtime;
    	  
	      final XYSeries iej_op = new XYSeries( "Inequality Join Optimized" );
    	  
	      for(int i=100; i<=1000; i = i+100) {
	      
	    	  System.out.println("Number of chunks: " + i);
	    	 
		      start = System.currentTimeMillis();
			  ParserTestIEJoin<SelfInequalityJoinTwoPredicateOptimized> test = new ParserTestIEJoin<SelfInequalityJoinTwoPredicateOptimized>("../../../../Output/query_2b.txt",
						"../../../../Output/Joined_Result_Query_2d_2b.txt","SelfInequalityJoinTwoPredicateOptimized", i);
		      end = System.currentTimeMillis();
		      
			  runtime = end - start;
		      
			  System.out.println("Runtime: "+runtime);
			  
		      iej_op.add( i , runtime );
	      
	      }
    	  
	      final XYSeriesCollection dataset = new XYSeriesCollection( );
	      dataset.addSeries( iej_op );
	
	      JFreeChart xylineChart = ChartFactory.createXYLineChart(
	         "Variation of the Runtime versus the Number of Chunks in the Optimized Self IE Join", 
	         "Number of Chunks",
	         "Runtime in ms", 
	         dataset,
	         PlotOrientation.VERTICAL, 
	         true, true, false);
	      
	      int width = 640;   /* Width of the image */
	      int height = 480;  /* Height of the image */ 
	      File XYChart = new File( "../../../../Report/XYLineChart4.jpeg" ); 
	      ChartUtilities.saveChartAsJPEG( XYChart, xylineChart, width, height);
   }
}
