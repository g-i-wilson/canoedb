package io.github.gabrielwilson3.canoedb.transforms;

import java.util.*;
import io.github.gabrielwilson3.canoedb.*;

public class Sum extends Transform {

	@Override
	public Collection<TableRow> tableRows (Table t, String column, String searchString) {
		// original results collection
		Object[] results = t
			.tableIndex
			.search( column, searchString )
			.toArray();
		// empty collection for a single customized TableRow
		Collection<TableRow> newCol = new ArrayList<>();
		int sum = 0;
		if (results.length>0) {
			// loop through the TableRows returned, and sum the column
			for (int i=0; i<results.length; i++) {
				TableRow tr = (TableRow) results[i];
				try {
					sum += Integer.parseInt( tr.read( column ) );
				} catch (Exception e) {
					// ignore
				}
			}
			// get the last TableRow of the results
			TableRow tr = (TableRow) results[ results.length-1 ];
			// create a cloned TableRow (deep clone of data)
			TableRow newTr = tr.cloned();
			// write the sum of the collection to newTr
			newTr.write( column, Integer.toString( sum ) );
			// add to the new Collection
			newCol.add( newTr );
		}
		return newCol;
	}

	
}