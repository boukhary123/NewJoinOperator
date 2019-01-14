package iterator;

import global.RID;

//this class is used to represent a tuple to be sorted in an array
class Row {
	public int heapfile_number; // heapfile number to ensure unique rid when using many heapfiles
	public RID rid; // the record id number
	public int field_to_sort; // the field to be sorted

	// Constructor
	public Row(int heapfile_number, RID rid, int field_to_sort) {
		this.heapfile_number = heapfile_number;
		this.rid = new RID();
		this.rid.copyRid(rid);
		this.field_to_sort = field_to_sort;
	}

	public boolean equals(Row right) {
		return ((this.heapfile_number == right.heapfile_number) && this.rid.equals(right.rid));
	}
}