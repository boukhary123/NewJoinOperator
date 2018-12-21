package iterator;

import global.RID;
import heap.Tuple;

// this class class is an extension of the class Row and we store in it 
// an extra element which is the tuple containing the fields to be selected
public class RowWithTuple extends Row {

	public Tuple field_to_select;

	public RowWithTuple(RID rid, int field_to_sort, Tuple field_to_select) {
		super(rid, field_to_sort);
		this.field_to_select = new Tuple(field_to_select);
	}
}
