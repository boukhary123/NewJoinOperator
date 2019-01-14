package iterator;

import java.util.ArrayList;

/**
*
* This class contains some functions that are used by the inequality joins.
*/

public class Utils {
	
	/**
	 * This function is used to compute the offset of array 1 with respect to array
	 * 2 using the sort-merge algorithm according to the sorting order.
	 * 
	 * @param arr1      an arrayList representing the first array
	 * @param arr2      an arrayList representing the second array
	 * @param offset    The offset array to be computed
	 * @param Ascending a boolean variable representing the sort order of the 2
	 *                  arrays
	 */
	public static void SortMerge(ArrayList<Row> arr1, ArrayList<RowWithTuple> arr2, int offset[], boolean Ascending) {
		// it is assumed arr1 and arr2 are sorted

		// get arr2 size
		int m = arr2.size();

		// get arr1 size
		int n = arr1.size();

		// initialize indexes
		int i = 0, j = 0;

		while (i < n) {

			// check if key from first array is equal to key from the second array
			if (arr1.get(i).field_to_sort == arr2.get(j).field_to_sort) {

				// j is the correct offset of the the element i from the first array
				offset[i] = j;

				// advance the first array
				i++;

				// advance the second array if the current element is the last one in the
				// second array
				if (j + 1 < m)
					j++;
				else
					// if the the current element is the last one in the second array
					// then keep the index as the last index
					j = m - 1;
			}

			// left element is less than (or greater depending on the sort order of the
			// arrays)
			// the element of the second array
			else if (Ascending ? arr1.get(i).field_to_sort < arr2.get(j).field_to_sort
					: arr1.get(i).field_to_sort > arr2.get(j).field_to_sort) {
				// then the element of arr1 should come before the element of arr2 if we were
				// merging both array
				offset[i] = j - 1;

				// advance the element of arr1
				i++;
			}

			else {
				// if neither of the first two cases are satisfied then we should advance both
				// arrays
				// only if the second array didn't reach its end otherwise it should remain in
				// its last
				// position
				if (j + 1 < m)
					j++;

				else {
					j = m - 1;
					offset[i] = j + 1;
					i++;
				}
			}
		}
	}

	/**
	 * This function is used to compute the offset of array 1 with respect to array
	 * 2 using the sort-merge algorithm according to the sorting order. Note that this function is the 
	 * same as the first array but takes different types of arrays.
	 * 
	 * @param arr1      an arrayList representing the first array
	 * @param arr2      an arrayList representing the second array
	 * @param offset    The offset array to be computed
	 * @param Ascending a boolean variable representing the sort order of the 2
	 *                  arrays
	 */
	public static void SortMergeWithTuple(ArrayList<RowWithTuple> arr1, ArrayList<Row> arr2, int offset[],
			boolean Ascending) {
		// it is assumed arr1 and arr2 are sorted

		// get arr2 size
		int m = arr2.size();

		// get arr1 size
		int n = arr1.size();

		// initialize indexes
		int i = 0, j = 0;

		while (i < n) {

			// check if key from first array is equal to key from the second array
			if (arr1.get(i).field_to_sort == arr2.get(j).field_to_sort) {

				// j is the correct offset of the the element i from the first array
				offset[i] = j;

				// advance the first array
				i++;

				// advance the second array if the current element is the last one in the
				// second array
				if (j + 1 < m)
					j++;
				else
					// if the the current element is the last one in the second array
					// then keep the index as the last index
					j = m - 1;
			}

			// left element is less than (or greater depending on the sort order of the
			// arrays)
			// the element of the second array
			else if (Ascending ? arr1.get(i).field_to_sort < arr2.get(j).field_to_sort
					: arr1.get(i).field_to_sort > arr2.get(j).field_to_sort) {
				// then the element of arr1 should come before the element of arr2 if we were
				// merging both array
				offset[i] = j - 1;

				// advance the element of arr1
				i++;
			}

			else {
				// if neither of the first two cases are satisfied then we should advance both
				// arrays
				// only if the second array didn't reach its end otherwise it should remain in
				// its last
				// position
				if (j + 1 < m)
					j++;

				else {
					j = m - 1;
					offset[i] = j + 1;
					i++;
				}
			}
		}
	}
	
	/**
	 * This function is used to compares 2 fields according to a specific operator
	 * 
	 * @param field1        The first integer field to compare
	 * @param field2        The second integer field to compare
	 * @param operator_type integer specifying the type of operator
	 * @return true if (field op1 field2)
	 */
	public static boolean predicate_evaluate(int field1, int field2, int operator_type) {

		// return the truth value of field1 op field2
		switch (operator_type) {
		case 1:
			return field1 < field2;
		case 2:
			return field1 > field2;

		case 4:
			return field1 <= field2;

		case 5:
			return field1 >= field2;
		}
		return false;

	}	

}
