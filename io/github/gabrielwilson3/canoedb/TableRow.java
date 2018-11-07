package io.github.gabrielwilson3.canoedb;

import java.util.*;

class TableRow {
	
	StringMap1D<String> 	data = new StringMap1D<>(); // column -> data_string
	List<TableRow>			to = new ArrayList<>(); // list of links to other TableRow objects
	List<TableRow>			from = new ArrayList<>(); // list of links to other TableRow objects
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
		return "[TableRow: "+table.name+":"+id+"]";
		//return "";
	}
	
	// link to another TableRow
	TableRow to (TableRow row) {
		to.add( row );
		return this;
	}
	// link to another TableRow
	TableRow from (TableRow row) {
		from.add( row );
		return this;
	}
	// get data
	String data ( String column ) {
		if (data.defined(column))
			return data.read(column);
		else return "";
	}
	// update data
	TableRow update ( StringMap1D<String> map ) {
		data.update( map );
		return this;
	}
	// set specific data element
	TableRow update ( String column, String dataElement ) {
		data.write( column, dataElement );
		return this;
	}
	// merge data
	TableRow merge ( StringMap1D<String> map ) {
		data.merge( map );
		return this;
	}
	
	
	// "kick-off" method, that creates the tablesTraversed object invokes the traverse method
	void read ( 
		StringMap2D<String> inputTemplate,
		StringMap2D<String> outputTemplate,
		StringMap3D<String> rowMap
	) {
		if (from.size() > 0) {
			// start traversing uphill toward an unknown number of peaks
			for (TableRow tr : from) {
				tr.read( inputTemplate, outputTemplate, rowMap );
			}
		} else {
			// start traversing downhill from this peak
			StringMap2D<String> inputMap = inputTemplate.cloned();
			StringMap2D<String> outputMap = outputTemplate.cloned();
			List<Table> tablesTraversed = new ArrayList<>();
			// loop through each path downhill from this peak
			for (TableRow tr : to) {
				if (traverseRead( inputMap, outputMap, tablesTraversed )) {
					rowMap.write( outputMap.map.toString(), outputMap.map );
				}
			}
		}
	}
	
	// Traverse method that starts at each "peak" and traverses down-hill to create an entire virtual row
	private boolean traverseRead (
		StringMap2D<String> inputMap, // transitions: populated -> null
		StringMap2D<String> outputMap, // transitions: null -> populated
		List<Table> tablesTraversed
	) {
		// name of this table
		String tableName = table.name;
		
		// log this traversal
		System.out.println( "TableRow: traversing "+tableName+":"+id );
		
		// record a reference to this table
		if (table!=null) tablesTraversed.add( table );
		
		// loop through input filters (if they exist)
		if (inputMap!=null) {
			// loop through the inputMap
			for (String column : inputMap.keys(tableName)) {
				String filter = inputMap.read(tableName, column);
				//if ( filter!=null && data.defined(column) ) { // filter is null if it's already been used
				if ( data.defined(column) ) { // filter is null if it's already been used
					// Check to see if there exists a filter that can disqualify this whole tableRow (that hasn't already been used)
					System.out.println( "TableRow: filter exists: "+tableName+":"+column );
					// Look up the inputMap data in the index for this table/column and see if it references back to this table
					if ( table.search( column, filter, false ).contains(this) ) {
						System.out.println( "TableRow: filter passed: "+tableName+", "+column );
						// if filter has been applied and is OK, then it is removed.
						// this allows us to not have to traverse the entire "virtual tableRow"...
						// ...if all filters have been used up and all results have been filled in.
						inputMap.write(tableName, column, null);
					} else {
						// if the filter fails, then disqualify this whole traverse and return false
						System.out.println( "TableRow: filter failed: "+tableName+":"+id+"."+column+"==\""+data.read(column)+"\" ? "+inputMap.toString() );
						return false;
					}
				}
				
				// are we done yet?
				if ( outputMap.noNulls() && inputMap.allNulls() ) return true;
			}
		}
		
		// loop through the outputMap
		for (String column : outputMap.keys(tableName)) {
			System.out.println("TableRow: column data: "+data.read(column));
			if ( data.defined(column) ) {
				// record the data as a key
				outputMap.write(tableName, column, data.read(column));
				System.out.println( "TableRow: outputMap: "+column+", "+data.read(column) );
			}
			
			// are we done yet?
			if ( outputMap.noNulls() && inputMap.allNulls() ) return true;
		}
			
		
		// traverse downhill through each "to" link
		for (TableRow tr : to) {
			System.out.println( "TableRow: TO "+tr.table.name+":"+tr.id+" FROM "+tableName+":"+id );
			
			// Make sure the row hasn't already been traversed (no endless loops allowed):
			if (tr.table==null || !tablesTraversed.contains(tr.table)) {
				// Call the referenced tableRow (fast-tracking any false return);
				if (! tr.traverseRead( inputMap, outputMap, tablesTraversed ) ) return false;
				// are we done yet?
				//if ( outputMap.noNulls() && inputMap.allNulls() ) return true;
			}
		}
		
		// ok, we're done with this TableRow...
		return true;
		
	}

}
