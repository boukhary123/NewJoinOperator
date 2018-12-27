package iterator;
import global.*;
import heap.*;
import diskmgr.*;
import bufmgr.*;
import index.*;
import java.io.*;
import java.util.Arrays;
import java.util.List;

/**
 *All the relational operators and access methods are iterators.
 */
public abstract class Iterator implements Flags {
  
	
	public static BufferedReader reader;
	
	public boolean firstHeapFileCall;
	
  /**
   * a flag to indicate whether this iterator has been closed.
   * it is set to true the first time the <code>close()</code> 
   * function is called.
   * multiple calls to the <code>close()</code> function will
   * not be a problem.
   */
  public boolean closeFlag = false; // added by bingjie 5/4/98

  /**
   *abstract method, every subclass must implement it.
   *@return the result tuple
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
  public abstract Tuple get_next() 
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
	   Exception;

  /**
   *@exception IOException I/O errors
   *@exception JoinsException some join exception
   *@exception IndexException exception from Index class
   *@exception SortException exception Sort class
   */
  public abstract void close() 
    throws IOException, 
	   JoinsException, 
	   SortException,
	   IndexException;
  
  /**
   * tries to get n_pages of buffer space
   *@param n_pages the number of pages
   *@param PageIds the corresponding PageId for each page
   *@param bufs the buffer space
   *@exception IteratorBMException exceptions from bufmgr layer
   */
  public void  get_buffer_pages(int n_pages, PageId[] PageIds, byte[][] bufs)
    throws IteratorBMException
    {
      Page pgptr = new Page();        
      PageId pgid = null;
      
      for(int i=0; i < n_pages; i++) {
	pgptr.setpage(bufs[i]);

	pgid = newPage(pgptr,1);
	PageIds[i] = new PageId(pgid.pid);
	
	bufs[i] = pgptr.getpage();
	
      }
    }

  /**
   *free all the buffer pages we requested earlier.
   * should be called in the destructor
   *@param n_pages the number of pages
   *@param PageIds  the corresponding PageId for each page
   *@exception IteratorBMException exception from bufmgr class 
   */
  public void free_buffer_pages(int n_pages, PageId[] PageIds) 
    throws IteratorBMException
    {
      for (int i=0; i<n_pages; i++) {
	freePage(PageIds[i]);
      }
    }

  private void freePage(PageId pageno)
    throws IteratorBMException {
    
    try {
      SystemDefs.JavabaseBM.freePage(pageno);
    }
    catch (Exception e) {
      throw new IteratorBMException(e,"Iterator.java: freePage() failed");
    }
    
  } // end of freePage

  private PageId newPage(Page page, int num)
    throws IteratorBMException {
    
    PageId tmpId = new PageId();
    
    try {
      tmpId = SystemDefs.JavabaseBM.newPage(page,num);
    }
    catch (Exception e) {
      throw new IteratorBMException(e,"Iterator.java: newPage() failed");
    }

    return tmpId;

  } // end of newPage
  
  public boolean getNextHeapFile(int R2_no_flds, AttrType[] R2types, String hf_filename) {
	  
	  boolean end = false;
	  Heapfile hf = null;
	  
		// create a tuple for heap file
		Tuple t = new Tuple();
	    try {
	      t.setHdr((short) R2_no_flds,R2types, null);
	    }
	    catch (Exception e) {
	      System.err.println("*** error in Tuple.setHdr() ***");
	      e.printStackTrace();
	    }
	    
	    int size = t.size();
	    
	    // inserting the tuple into file "sailors"
	    RID             rid;
	    hf = null;
	    try {
	    	// create heap file for R2
	    	hf = new Heapfile(hf_filename);
	    }
	    catch (Exception e) {
	      System.err.println("*** error in Heapfile constructor ***");
	      e.printStackTrace();
	    }
	    
	    t = new Tuple(size);
	    try {
	      t.setHdr((short) R2_no_flds, R2types, null);
	    }
	    catch (Exception e) {
	      System.err.println("*** error in Tuple.setHdr() ***");
	      e.printStackTrace();
	    }
		
	
	      try {
				int count = 0;
		  		String rec;
		  		
		  		if(firstHeapFileCall) {
		  		rec = reader.readLine();
		  		firstHeapFileCall = false;
		  		}
		  		
		  		while(count < 10000) {
		  			
		  		if((rec = reader.readLine())==null) {
		  			end = true;
		  			break;
		  		}
	  			// read each field for each tuple
	  			List<String> fields = Arrays.asList(rec.split(","));	
	  			
	  			for(int k=0; k<R2_no_flds;k++) {
	  			
	  			t.setIntFld(k+1, Integer.parseInt(fields.get(k)));
	  			
	  			}
			  		
			        try {
			        	// insert the type into the heap file
			        	rid = hf.insertRecord(t.returnTupleByteArray());
			        	count++;
			              }
			              catch (Exception e) {
			        	System.err.println("*** error in Heapfile.insertRecord() ***");
			        	
			        	e.printStackTrace();
			              }  
		  		}
	  		
	      }
	      catch (Exception e) {
		System.err.println("*** Heapfile error in Tuple.setStrFld() ***");
		e.printStackTrace();
	      }
	  
	  return end;
  }
}
