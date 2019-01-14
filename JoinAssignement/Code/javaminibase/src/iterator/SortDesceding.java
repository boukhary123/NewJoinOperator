package iterator;

import java.util.Comparator;

class SortDesceding implements Comparator<Row> {
	// Used for sorting in descending order of
	// field_to_sort
	public int compare(Row a, Row b) {
		return -(a.field_to_sort - b.field_to_sort);
	}
}
