package io.github.gabrielwilson3.canoedb;

import java.util.*;
import java.math.BigInteger;

class Table {
	
	// Table properties
	String 					tableName;
	String[] 				tablecolumns;
	Map<String, String> 	referenceMap;
	Map<String, String> 	onReadMap;
	Map<String, String> 	onWriteMap;
	
	// Table rows
	Map<String, TableRow> 	rowIdMap = new HashMap<>();
	Map<String, TableRow>	rowDataMap = new HashMap<>();
	BigInteger 				highestRowId = new BigInteger("1");
	
	// Table file
	TableFile				tableFile;

	// Table index
	TableIndex 				tableIndex = new TableIndex();
	
	// Database object containing this Table
	Database				dbObject;
	
	// Connect table to a physical file
	Table ( Database d, TableFile f ) {
		// receive the Database object
		this.dbObject = d;
		// receive the TableFile object
		this.tableFile = f;
		// link to the header structure
		this.tableName = this.tableFile.name();
		this.referenceMap = MapFunctions.twoStringArrays( this.tableFile.columns, this.tableFile.references );
		this.onReadMap = MapFunctions.twoStringArrays( this.tableFile.columns, this.tableFile.onRead );
		this.onWriteMap = MapFunctions.twoStringArrays( this.tableFile.columns, this.tableFile.onWrite );
		// link to the data
		this.tableFile.data.forEach(( id, data )->{
			this.newRow( id, data );
		});
	}
	
	// Append a row with a partial (or complete) row map
	// Converts down to String[] > appends to file > newRow converts it back up to Map<>
	boolean append( Map<String, String> newRowMap ) {
		
		// Produce a String[] using the Map object
		String[] newData = new String[this.columns().length];
		for (int i=0; i<this.columns().length; i++) {
			if(newRowMap.containsKey(this.columns()[i])) {
				newData[i] = newRowMap.get(this.columns()[i]);
			} else {
				newData[i] = "";
			}
		}
		
		// Try to append to the physical TableFile
		String id = this.nextRowId();
		if (this.tableFile.append( id, newData )){
			// If that works, then call newRow()
			this.newRow( id, newData );
			return true;
		} else {
			return false;
		}
	}

	
	// Spawn a TableRow object
	TableRow newRow ( String id, String[] data ) {
		// new row
		TableRow rowObject = new TableRow( id, data, this );
		// record the row by id and by hash-str
		this.rowIdMap.put( rowObject.id(), rowObject );
		this.rowDataMap.put( rowObject.str(), rowObject );
		// Increment the highest row integer
		this.checkRowId( rowObject.id() );
		System.out.println( rowObject.id() );
		// update the TableIndex
		for (String column : this.columns()) {
			// column -> data -> TableRowObject
			this.tableIndex.appendRow( column, rowObject.field( column ), rowObject );
		}
		// return a reference to the row object
		return rowObject;
	}

	
	// Pull a new row id
	String nextRowId () {
		// increment the highestRowId
		this.highestRowId = this.highestRowId.add( BigInteger.ONE );
		// return the new id
		if ( this.rowIdMap.containsKey(this.highestRowId.toString()) ) {
			// recursion
			return this.nextRowId();
		} else {
			return this.highestRowId.toString();
		}
	}
	
	// check the highestRowId
	void checkRowId( String id ) {
		// check the id to see if highestRowId is equal or higher; replace otherwise
		try {
			BigInteger toCheck = new BigInteger( id );
			if (this.highestRowId.compareTo(toCheck) < 0) {
				this.highestRowId = toCheck;
			}
		} catch (Exception e) {
			// ignore exception
		}
		System.out.println( id+", "+highestRowId.toString() );

	}
	
	// get a row object by ID
	TableRow getRow (String id) {
		return this.rowIdMap.get(id);
	}

	// Filtered maps of TableRow objects
	Map<String, TableRow> index (String column, String dataElement) {
		System.out.println( "Table: index input: "+column+","+dataElement );
		Map<String, TableRow> filteredRows = this.tableIndex.filterRows( column, dataElement );
		System.out.println( "Table: index output: "+filteredRows.toString() );
		return filteredRows;
	}

	// Name of the containing database:
	Database db () {
		return this.dbObject;
	}
	
	// Name of this table
	String name () {
		return this.tableName;
	}

	// columns of this table
	String[] columns () {
		return this.tableFile.columns();
	}

	// Tables referenced by this table
	String references (String column) {
		return this.referenceMap.get( column );
	}
	
	// Transformation to occur on data before being read
	String onRead (String column) {
		return this.onReadMap.get( column );
	}
	
	// Transformation to occur on data before being written
	String onWrite (String column) {
		return this.onWriteMap.get( column );
	}
	
	
}
