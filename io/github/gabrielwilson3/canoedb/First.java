package io.github.gabrielwilson3.canoedb;

import java.util.*;

class First extends Transform {

	@Override
	Collection<TableRow> tableRows (Table t, String column, String searchString) {
		// utilizes the search() function of the tableIndex that finds TableRows based on whole data element or word-begins-with fragment
		Object[] results = t
			.tableIndex
			.search( column, searchString )
			.toArray();
		Collection<TableRow> first = new ArrayList<>();
		TableRow tr = (TableRow) results[0];
		if (tr!=null) first.add( tr );
		return first;
	}
	
}