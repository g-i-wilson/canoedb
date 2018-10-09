package io.github.gabrielwilson3.canoedb;

import java.util.*;

class TableRow {
	
	Map<String, String> 	dataMap;
	String					rowId;
	String					rowData;
	Table					tableObject;
	

	TableRow (String id, String[] data, Table t) {
		// Record the reference to the Table object
		this.tableObject = t;
		// Record the id of this row
		this.rowId = id;
		// Record a concatenated string of this row's data
		this.rowData = String.join( "", data );
		// Map data into dataMap via columns, and checking for missing data
		this.dataMap = MapFunctions.twoStringArrays( this.tableObject.columns(), data );
	}
	
	String field (String column) {
		return this.dataMap.get( column );
	}
	
	Table table () {
		return this.tableObject;
	}
	
	String id () {
		return this.rowId;
	}

	String str () {
		return this.rowData;
	}
	
	// "kick-off" method, that creates the rowsTraversed object invokes the traverseRows method
	boolean getData ( 	Map<String, Map<String, String>> filterMap,
					Map<String, Map<String, String>> resultMap ) {
		// false only if nothing to do...
		if (resultMap==null) return false;
		Map<TableRow, TableRow> rowsTraversed = new HashMap<>();
		// false only if a filter disqualifies this traverse...
		return this.traverseRows( filterMap, resultMap, rowsTraversed );
	}
	
	// Traverse method that gathers data from this TableRow, and any TableRow referenced
	boolean traverseRows ( Map<String, Map<String, String>> filterMap,
						Map<String, Map<String, String>> resultMap,
						Map<TableRow, TableRow> rowsTraversed ) {
		
		// make sure this tableRow hasn't already been traversed (no endless loops allowed):
		if (rowsTraversed.containsValue(this)) {
			return true;
		}
		
		System.out.println( "TableRow: traversing "+this.tableObject.name()+":"+this.id().toString() );
		
		// loop through each column in this tableRow and fill in the resultMap
		for (String column : dataMap.keySet()) {
			String data = dataMap.get(column);

			// if a column is called out in the resultMap,
			// then add that data (but only if null).
			String tableName = this.tableObject.name();
		
			// Check to see if there exists a filter that can disqualify this whole tableRow
			if (
				filterMap != null
				&& filterMap.containsKey(tableName)
				&& filterMap.get(tableName).containsKey(column)
			) {
				// Look up the filterMap data in the index for this table/column and see if it references back to this table
				if ( this.tableObject.index( column, filterMap.get(tableName).get(column) ).containsValue(this) ) {
					// if filter has been applied and is OK, then it is removed.
					// this allows us to not have to traverse the entire "virtual tableRow"...
					// ...if all filters have been used up and all results have been filled in.
					filterMap.get(tableName).put(column, null);
				} else {
					// if the filter fails, then all we need to do is bail-out by returning false.
					System.out.println( "TableRow filter: "+this.tableObject.name()+":"+this.id().toString()+" -> "+resultMap.toString() );
					return false;
				}
			}

			if (
				resultMap.containsKey(tableName)
				&& resultMap.get(tableName).containsKey(column)
			) {
				resultMap.get(tableName).put(column, data);
				System.out.println( "TableRow result: "+this.tableObject.name()+":"+this.id().toString()+" -> "+resultMap.toString() );
			}
			
			// return early if there's nothing left to do
			if ( this.noNulls(resultMap) && this.allNulls(filterMap) ) return true;			
			
		}
		
			
		// re-loop through each column in this tableRow and check for table references
		for (String column : dataMap.keySet()) {
			String rowId = dataMap.get(column);
			//System.out.println( "TableRow: reference loop: table="+this.tableObject.name()+" ID="+this.id().toString()+" "+resultMap.toString() );

			// if a column contains a reference to a tableRow (that is not already in the rowsTraversed),
			// then call traverseRows on that tableRow object.
			String nameReference = this.tableObject.references(column);
			if (! nameReference.equals("")) {
				// There exists a tableName string
				Table tableReferenced = this.tableObject.db().tables(nameReference);
				if (tableReferenced != null) {
					// The tableName does reference a valid table
					TableRow trReferenced = tableReferenced.getRow(rowId);
					if (trReferenced != null) {
						// The row ID does reference a valid table row
						rowsTraversed.put( trReferenced, this ); // to_row_referenced -> from_this_row
						System.out.println( "TableRow: TO "+nameReference+":"+rowId+" FROM "+this.tableObject.name()+":"+this.id() );
						// Call the referenced tableRow (fast-tracking any false return);
						if ( ! trReferenced.traverseRows( filterMap, resultMap, rowsTraversed ) ) return false;
					}
				}
			}
			
			// return early if there's nothing left to do
			if ( this.noNulls(resultMap) && this.allNulls(filterMap) ) return true;			
			
		}
		
		// if we haven't returned yet for any other reason, then return true...
		return true;
		
	}
	
	// function to check for existance of nulls
	boolean noNulls (Map<String, Map<String, String>> map) {
		if (map==null) return false;
		for (String a : map.keySet()) {
			for (String b : map.get(a).keySet()) {
				if (map.get(a).get(b) == null) return false;
			}
		}
		return true;
	}

	// function to check for non-existance of nulls
	boolean allNulls (Map<String, Map<String, String>> map) {
		if (map==null) return true;
		for (String a : map.keySet()) {
			for (String b : map.get(a).keySet()) {
				if (map.get(a).get(b) != null) return false;
			}
		}
		return true;
	}

}
