package io.github.gabrielwilson3.canoedb;

import java.util.*;

class TableRow {
	
	StringMap1D<String> 	data = new StringMap1D<>(); // column -> data_string
	List<TableRow>			links = new ArrayList<>(); // list of links to other TableRow objects
	String					id = ""; // the ID string from the Table
	Table					table; // the Table object
	
	// initialize blank
	TableRow () {}
	// initialize with data
	TableRow ( Table t, String i, StringMap1D<String> d ) {
		table = t;
		id = i;
		data = d;
	}

	// toString
	@Override
	public String toString () {
		return table.name+":"+id;
		//return "";
	}
	
	// link to another TableRow
	TableRow link (TableRow row) {
		links.add( row );
		return this;
	}
	// get data
	String data ( String column ) {
		if (data.defined(column))
			return data.read(column);
		else return "";
	}
	// set data
	TableRow update ( StringMap1D<String> map ) {
		data.update( map );
		return this;
	}
	// set data
	TableRow merge ( StringMap1D<String> map ) {
		data.merge( map );
		return this;
	}
	
	
	// "kick-off" method, that creates the rowsTraversed object invokes the traverse method
	boolean read ( 
		StringMap2D<String> inputMap, // transitions: populated -> null
		StringMap2D<String> outputMap // transitions: null -> populated
	) {
		// false only if nothing to do...
		if (outputMap==null) return false;
		Map<TableRow, TableRow> rowsTraversed = new HashMap<>();
		// false only if a filter disqualifies this traverse...
		return traverseRead( inputMap, outputMap, rowsTraversed );
	}
	
	
	// Traverse method that gathers data from this TableRow, and any TableRow referenced
	private boolean traverseRead (
		StringMap2D<String> inputMap,
		StringMap2D<String> outputMap,
		Map<TableRow, TableRow> rowsTraversed
	) {
		// make sure this tableRow hasn't already been traversed (no endless loops allowed):
		if (rowsTraversed.containsValue(this)) return true;
		
		// name of this table
		String tableName = table.name;
		
		// log this traversal
		System.out.println( "TableRow: traversing "+tableName+":"+id );
		
		// loop through the inputMap
		for (String column : inputMap.keys(tableName)) {
			if ( data.defined(column) ) {
				// Check to see if there exists a filter that can disqualify this whole tableRow
				System.out.println( "TableRow: filter defined: "+tableName+", "+column );
				// Look up the inputMap data in the index for this table/column and see if it references back to this table
				if ( table.search( column, inputMap.read(tableName, column) ).contains(this) ) {
					System.out.println( "TableRow: filter passed: "+tableName+", "+column );
					// if filter has been applied and is OK, then it is removed.
					// this allows us to not have to traverse the entire "virtual tableRow"...
					// ...if all filters have been used up and all results have been filled in.
					inputMap.write(tableName, column, null);
				} else {
					// if the filter fails, then all we need to do is bail-out by returning false.
					System.out.println( "TableRow: filter failed: "+tableName+":"+id+" -> "+outputMap.toString() );
					return false;
				}
			}
			
			// are we done yet?
			if ( outputMap.noNulls() && inputMap.allNulls() ) return true;
		}
			
		// loop through the outputMap
		for (String column : outputMap.keys(tableName)) {
			if ( data.defined(column) ) {
				outputMap.write(tableName, column, data.read(column));
				System.out.println( "TableRow: outputMap: "+column+", "+data.read(column) );
			}
			
			// are we done yet?
			if ( outputMap.noNulls() && inputMap.allNulls() ) return true;
		}
			
		
		// traverse each connected TableRow
		for (TableRow t : links) {
			rowsTraversed.put( t, this ); // to_row_referenced -> from_this_row
			System.out.println( "TableRow: TO "+t.table.name+":"+t.id+" FROM "+tableName+":"+id );
			// Call the referenced tableRow (fast-tracking any false return);
			if ( ! t.traverseRead( inputMap, outputMap, rowsTraversed ) ) return false;
			
			// are we done yet?
			if ( outputMap.noNulls() && inputMap.allNulls() ) return true;		
		}
		
		// ok, we're done with this TableRow...
		return true;
		
	}

}
