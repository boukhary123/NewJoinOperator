package iterator;

import java.util.Comparator;

class SortAsceding implements Comparator<Row> {
	// Used for sorting in ascending order of
	// field_to_sort
	public int compare(Row a, Row b) {
		return a.field_to_sort - b.field_to_sort;

	}
}
