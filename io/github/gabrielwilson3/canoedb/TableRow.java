package io.github.gabrielwilson3.canoedb;

import java.util.*;
import io.github.gabrielwilson3.canoedb.transforms.*;

public class TableRow {
	
	StringMap1D<String> 	data = new StringMap1D<>(); // column -> data_string
	StringMap1D<Transform>	transform = new StringMap1D<>();
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
		transform = table.transformMap;
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
	
	// columns
	Set<String> columns () {
		return data.keys();
	}
	
	// WRITE a StringMap1D
	TableRow write ( StringMap1D<String> map ) {
		for (String column : map.keys()) {
			write( column, data.write( column, map.read( column ) ) );
		}
		return this;
	}
	// WRITE a specific data element (optional Transform override)
	TableRow write ( String column, String dataElement, Transform tran ) {
		if (tran!=null)
			data.write( column, tran.onWrite(dataElement) );
		else
			write( column, dataElement );
		return this;
	}
	// WRITE a specific data element
	TableRow write ( String column, String dataElement ) {
		if (transform.defined(column))
			data.write( column, transform.read(column).onWrite(dataElement) );
		else
			data.write( column, dataElement );
		return this;
	}
	// READ a specific data element (ignoring Query)
	String read ( String column ) {
		if (transform.defined(column))
			return transform.read(column).onRead( data.read(column) );
		else
			return data.read( column );
	}
	// READ a specific data element (checking for override Transform in Query)
	String read ( String column, Query q ) {
		String tableName = table.name;
		if (q.transformMap.defined(tableName, column)) {
			Transform tran = q.transformMap.read(tableName, column);
			return tran.onRead( data.read(column) );
		} else {
			return read(column);
		}
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
				q.log( "TableRow: / UPHILL: "+table.name+":"+id+" -> "+tr.table.name+":"+tr.id );
				tr.read( q );
			}
		} else {
			// make sure this is a new "origin" to start downhill from
			if (q.readOrigins.contains(this)) return;
			q.readOrigins.add(this);
			// start traversing downhill from this peak
			q.log( "TableRow: * PEAK: "+table.name+":"+id );
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
			if ( data.defined(column) ) { // filter is null if it's already been used
				q.log( "TableRow: filter defined: "+tableName+"."+column );
				// AND logic (must pass all filters)
				if (q.logic.equals("and")) {
					if ( q.rows( table, column, filter ).contains(this) ) {
						q.log( "TableRow: filter PASSED (AND): "+filter );
						inputMap.write(tableName, column, null);
					} else {
						q.log( "TableRow: filter FAILED (AND): "+filter );
						return false;
					}
				// XOR logic (must fail all filters)
				} else if (q.logic.equals("xor")) {
					if ( q.rows( table, column, filter ).contains(this) ) {
						q.log( "TableRow: filter PASSED (bad) (XOR): "+filter );
						inputMap.write(tableName, column, null);
						return false;
					} else {
						q.log( "TableRow: filter FAILED (good) (XOR): "+filter );
					}
				}
				// OR logic (all filters ignored)
			}
			
			// are we done yet?
			if ( outputMap.noNulls() && inputMap.allNulls() ) return true;
		}
		
		// loop through the output blanks
		for (String column : outputMap.keys(tableName)) {
			// reference any data Strings found in outputMap
			if ( data.defined(column) ) {
				outputMap.write(tableName, column, read( column, q )); // check for override Transform in Query
			}
			// are we done yet?
			if ( outputMap.noNulls() && inputMap.allNulls() ) return true;
		}
			
		
		// traverse downhill through each "to" link
		for (TableRow tr : to) {
			// Make sure the row hasn't already been traversed (no endless loops allowed):
			if (tr.table==null || !tablesTraversed.contains(tr.table)) {
				q.log( "TableRow: \\ DOWNHILL: "+tableName+":"+id+" -> "+tr.table.name+":"+tr.id );
				// Call the referenced tableRow (fast-tracking any false return);
				if (! tr.traverseRead( inputMap, outputMap, tablesTraversed, q ) ) return false;
			}
		}
		
		// ok, we're done with this TableRow...
		return true;
		
	}

}
