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
	
	// "hash" string unique to this TableRow (how this is implemented may change)
	String hash () {
		return data.hash();
	}
	
	// link TO
	boolean linkTo ( TableRow tr ) {
		if (!to.contains(tr)) {
			to.add(tr);
			return true;
		} else return false;
	}
	// link FROM
	boolean linkFrom ( TableRow tr ) {
		if (!from.contains(tr)) {
			from.add( tr );
			return true;
		} else return false;
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
	
	
	// READ UPHILL traverse (recursive)
	void read ( Query q ) {
		if (from.size() > 0) {
			// start traversing uphill toward an unknown number of peaks
			for (TableRow tr : from) {
				System.out.println( "TableRow: / UPHILL: "+table.name+":"+id+" -> "+tr.table.name+":"+tr.id );
				q.time();
				tr.read( q );
			}
		} else {
			// start traversing downhill from this peak
			System.out.println( "TableRow: * PEAK: "+table.name+":"+id );
			StringMap2D<String> inputMap = q.inputTemplate.cloned();
			StringMap2D<String> outputMap = q.outputTemplate.cloned();
			List<Table> tablesTraversed = new ArrayList<>();
			if (traverseRead( inputMap, outputMap, tablesTraversed, q ))
				q.rowMap.write( outputMap.hash(), outputMap.map );
		}
	}
	
	// READ DOWNHILL traverse (recursive)
	private boolean traverseRead (
		StringMap2D<String> inputMap, // transitions: populated -> null
		StringMap2D<String> outputMap, // transitions: null -> populated
		List<Table> tablesTraversed,
		Query q
	) {
		// name of this table
		String tableName = table.name;
		
		// record a reference to this table
		if (table!=null) tablesTraversed.add( table );
		
		// loop through input filters (if they exist)
		for (String column : inputMap.keys(tableName)) {
			String filter = inputMap.read(tableName, column);
			//if ( filter!=null && data.defined(column) ) { // filter is null if it's already been used
			if ( data.defined(column) ) { // filter is null if it's already been used
				System.out.println( "TableRow: filter defined: "+tableName+"."+column );
				q.time();
				// AND logic (must pass all filters)
				if (q.logic.equals("and")) {
					if ( table.search( column, filter ).contains(this) ) {
						System.out.println( "TableRow: filter PASSED (AND): "+filter );
						inputMap.write(tableName, column, null);
					} else {
						System.out.println( "TableRow: filter FAILED (AND): "+filter );
						return false;
					}
				// XOR logic (must fail all filters)
				} else if (q.logic.equals("xor")) {
					if ( table.search( column, filter ).contains(this) ) {
						System.out.println( "TableRow: filter PASSED (bad) (XOR): "+filter );
						inputMap.write(tableName, column, null);
						return false;
					} else {
						System.out.println( "TableRow: filter FAILED (good) (XOR): "+filter );
					}
				}
				// OR logic (all filters ignored)
				q.time();
			}
			
			// are we done yet?
			if ( outputMap.noNulls() && inputMap.allNulls() ) return true;
		}
		
		// loop through the output blanks
		for (String column : outputMap.keys(tableName)) {
			if ( data.defined(column) ) {
				// record the data as a key
				outputMap.write(tableName, column, data.read(column));
				//System.out.println( "TableRow: wrote to outputMap: "+column+", "+data.read(column) );
			}
			
			// are we done yet?
			if ( outputMap.noNulls() && inputMap.allNulls() ) return true;
		}
			
		
		// traverse downhill through each "to" link
		for (TableRow tr : to) {
			// Make sure the row hasn't already been traversed (no endless loops allowed):
			if (tr.table==null || !tablesTraversed.contains(tr.table)) {
				System.out.println( "TableRow: \\ DOWNHILL: "+tableName+":"+id+" -> "+tr.table.name+":"+tr.id );
				// Call the referenced tableRow (fast-tracking any false return);
				if (! tr.traverseRead( inputMap, outputMap, tablesTraversed, q ) ) return false;
				// are we done yet?
				//if ( outputMap.noNulls() && inputMap.allNulls() ) return true;
			}
		}
		
		// ok, we're done with this TableRow...
		return true;
		
	}

}
