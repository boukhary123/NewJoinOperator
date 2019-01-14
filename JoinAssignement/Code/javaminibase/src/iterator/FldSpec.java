package iterator;
import heap.*;

public class FldSpec {
  public  RelSpec relation;
  public  int offset;

  /**constructor
   *@param _relation the relation is outer or inner
   *@param _offset the offset of the field
   */
  public  FldSpec(RelSpec _relation, int _offset)
    {
      relation = _relation;
      offset = _offset;
    }

/**constructor
 *@param _fldSpec the FldSpec is outer or inner
 */
public  FldSpec(FldSpec fldspec)
  {
	relation = new RelSpec(fldspec.relation.key);
	offset = fldspec.offset;

  }
}
