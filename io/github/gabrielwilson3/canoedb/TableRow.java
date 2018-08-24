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
	
	// "kick-off" method, that invokes the traverseRows method
	void getData ( Map<String, Map<String, String>> resultMap ) {
		Map<TableRow, TableRow> rowsTraversed = new HashMap<>();
		this.traverseRows( resultMap, rowsTraversed );
	}
	
	// Traverse method that gathers data from this TableRow, and any TableRow referenced
	void traverseRows ( Map<String, Map<String, String>> resultMap, Map<TableRow, TableRow> rowsTraversed ) {
		
		// make sure this tableRow hasn't already been traversed (no endless loops allowed):
		if (rowsTraversed.containsValue(this)) {
			return;
		}
		
		System.out.println( "TableRow: traversing "+this.tableObject.name()+":"+this.id().toString() );
		
		// loop through each column in this tableRow and fill in the resultMap
		for (String column : dataMap.keySet()) {
			String data = dataMap.get(column);
		
			// if a column is called out in the resultMap,
			// then add that data (if null).
			String tableName = this.tableObject.name();
			if (resultMap.containsKey(tableName) && resultMap.get(tableName).containsKey(column)) {
				resultMap.get(tableName).put(column, data);
				System.out.println( "TableRow: "+this.tableObject.name()+":"+this.id().toString()+" -> "+resultMap.toString() );
			}
			
			// exit loop if no more null values remain in resultMap
			if (this.resultComplete( resultMap )) break;
			
		}
		
			
		// re-loop through each column in this tableRow and check for table references
		for (String column : dataMap.keySet()) {
			String rowId = dataMap.get(column);
			//System.out.println( "TableRow: reference loop: table="+this.tableObject.name()+" ID="+this.id().toString()+" "+resultMap.toString() );

			// if a column contains a reference to a tableRow (that is not already in the rowsTraversed),
			// then call traverseRows on that tableRow object.
			String nameReference = this.tableObject.references(column);
			if (!nameReference.equals("")) {
				// There exists a tableName string
				Table tableReferenced = this.tableObject.db().tables(nameReference);
				if (tableReferenced != null) {
					// The tableName does reference a valid table
					TableRow trReferenced = tableReferenced.getRow(rowId);
					if (trReferenced != null) {
						// The row ID does reference a valid table row
						rowsTraversed.put( trReferenced, this ); // to_row_referenced -> from_this_row
						System.out.println( "TableRow: TO "+nameReference+":"+rowId+" FROM "+this.tableObject.name()+":"+this.id() );
						trReferenced.traverseRows( resultMap, rowsTraversed );
					}
				}
			}
			
			// exit loop if no more null values remain in resultMap
			if (this.resultComplete( resultMap )) break;			
			
		}
		
	}
	
	// function to check for result complete
	boolean resultComplete (Map<String, Map<String, String>> resultMap) {
		// loop through tables
		for (String table : resultMap.keySet()) {
			Map<String, String> columnMap = resultMap.get(table);
			// loop through columns
			for (String column : columnMap.keySet()) {
				// get data string using the column
				if (columnMap.get(column) == null) return false;
			}
		}
		return true;
	}

}
