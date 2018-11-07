package io.github.gabrielwilson3.canoedb;

import java.util.*;
import java.math.BigInteger;
import java.nio.file.*;
import java.io.File;


class Table {
	
	// Table properties
	String 					name;
	String[] 				columns;
	StringMap1D<String>		columnMap;
	StringMap1D<String> 	referenceMap;
	StringMap1D<String> 	onReadMap;
	StringMap1D<String> 	onWriteMap;
	
	// Table rows
	StringMap1D<TableRow> 	rowIdMap = new StringMap1D<>();
	StringMap1D<TableRow> 	rowDataMap = new StringMap1D<>();
	BigInteger 				highestRowId = new BigInteger("1");
	
	// Table file
	File					tableFile;

	// Table index
	TableIndex 				tableIndex = new TableIndex();
	
	// Null Map
	StringMap1D<TableRow> 	null_map = new StringMap1D<>();
	
	// file exists
	boolean					fileExists = false;
	
	// null set
	Set<String>				null_set = new LinkedHashSet<>();
	
	// null_collection
	Collection<TableRow> 	null_collection = new ArrayList<>();
	

	// set the physical file
	Table ( String filePath ) {
		tableFile = new File( filePath );
		init();
	}
	Table ( File f ) {
		tableFile = f;
		init();
	}
	void init () {
		String fileName = tableFile.getName();
		int pos = fileName.lastIndexOf(".");
		if (fileName.lastIndexOf(".") > 0) {
			fileName = fileName.substring(0, pos);
		}
		name = fileName;
		System.out.println("Table: initialized "+name);
	}
	
	// Load table from a physical file
	boolean read () {
		// read CSV file
		try (Scanner sc = new Scanner(tableFile, "UTF-8")) {
			// initialize the basic table info maps
			columns			= removeFirst( decodeLine(sc.nextLine()) );
			columnMap		= blankMap( columns );
			referenceMap	= twoArraysMap( columns, removeFirst(decodeLine(sc.nextLine())) );
			onReadMap		= twoArraysMap( columns, removeFirst(decodeLine(sc.nextLine())) );
			onWriteMap		= twoArraysMap( columns, removeFirst(decodeLine(sc.nextLine())) );
			fileExists = true;
			// Loop through lines and fill fileMap with TableRow objects
			while (sc.hasNextLine()) {
				// read a CSV data line: id,data1,dataN
				String[] 	data 	= decodeLine( sc.nextLine() );
				String 		rowId 	= data[0];
				String[] 	rowData = removeFirst( data );
				// spawn a TableRow
				TableRow 	tr 		= new TableRow( this, rowId, twoArraysMap( columns, rowData ) );
				//System.out.println("Table: read row "+tr.data.toString());
				checkRowId( tr.id );
				logTableRow( tr );
			}
			System.out.println("Table: rows: "+rowIdMap.toString());
			// Scanner suppresses io exceptions
			if (sc.ioException() != null) System.out.println( "Table: file io exception: "+sc.ioException() );
		} catch (Exception e) {
			System.out.println( "Table: file Scanner exception: "+e );
			e.printStackTrace();
			return false;
		}
		// memory
		printMemoryUsage();
		// able to read file
		return true;
	}
	
	// get a Set of all the TableRow objects in this Table
	Set<String> rows () {
		return rowIdMap.keys();
	}
	
	// get a map of TableRow objects based on column -> data_fragment
	Collection<TableRow> search (String column, String dataFragment, boolean writeMode) {
		System.out.println( "Table: index input: "+column+","+dataFragment );
		Collection<TableRow> results = tableIndex.search( column, dataFragment );
		if (results!=null) {
			return results;
		} else if (writeMode) {
			TableRow tr = row();
			tr.update( column, dataFragment );
			tableIndex.write(tr);
			return tableIndex.search( column, dataFragment ); // must return non-null now...
		} else {
			return null_collection;
		}
	}
	
	// Creates a new table row (virtual; not yet appended)
	TableRow row () {
		// spawn a new row
		TableRow tr = new TableRow(this, nextRowId(), columnMap.cloned());
		System.out.println( "Table "+name+": added row "+tr.id+" (auto-ID)" );
		return tr;
	}

	// Get a TableRow by ID or create an empty TableRow for that ID
	TableRow row ( String id ) {
		if (rowIdMap.defined(id)) {
			// access a real row
			return rowIdMap.read(id);
		} else {
			// spawn a new row (virtual; not yet appended)
			TableRow tr = new TableRow(this, id, columnMap.cloned());
			System.out.println( "Table "+name+": added row "+tr.id );
			logTableRow( tr );
			return tr;
		}
	}
	
	// Get a TableRow by data 
	TableRow row ( StringMap1D<String> data ) {
		if (rowDataMap.defined(data.toString())) {
			return rowDataMap.read(data.toString());
		} else {
			TableRow tr = row();
			tr.update( data );
			append( tr );
			checkRowId( tr.id );
			logTableRow( tr );
			return tr;
		}
	}
			
	// append to the end of the Table file
	boolean append (TableRow tr) {
		// check for Table file existance
		if (!fileExists) {
			// use the data structure from this TableRow to initialize Table columns
			columnMap = tr.data.templated( "" ); // returns new templated object initialized to "" values
			referenceMap = columnMap; // we can just reference this same object for the rest...
			onReadMap = columnMap;
			onWriteMap = columnMap;
			// create a file header string
			String str =
				","+name+"\n"+
				","+String.join(",", columnMap.keys())+"\n"+
				",\n"+
				",\n"+
				",";
			try {
				Files.write(tableFile.toPath(), str.getBytes());
				fileExists = true;
				System.out.println( "Table "+name+": written to file "+tableFile );
			} catch (Exception e) {
				System.out.println("Table: ERROR writing to file "+tableFile);
				System.out.println(e);
				e.printStackTrace();
				return false;
			}
		}
		// make sure the TableRow has been logged
		logTableRow( tr );
		// append the TableRow to the Table file
		String str = "\n"+tr.data.join(",");
		try {
			Files.write(tableFile.toPath(), str.getBytes(), StandardOpenOption.APPEND);
			System.out.println( "Table "+name+": appended row "+tr.id );
		} catch (Exception e) {
			System.out.println("Table: ERROR appending to file "+tableFile);
			System.out.println(e);
			e.printStackTrace();
			return false;
		}
		return true;
	}
		
	// columns in Table
	public String[] columns () {
		return columns;
	}

	// referenced Table
	String reference (String column) {
		if (referenceMap!=null && referenceMap.defined(column))
			return referenceMap.read(column);
		else return "";
	}

	// onRead command
	String onRead (String column) {
		if (onReadMap!=null && onReadMap.defined(column))
			return onReadMap.read(column);
		else return "";
	}

	// onWrite command
	String onWrite (String column) {
		if (onWriteMap!=null && onWriteMap.defined(column))
			return onWriteMap.read(column);
		else return "";
	}
	
	// log a TableRow in the ID & data maps, and also the index
	private void logTableRow ( TableRow tr ) {
		rowIdMap.write( tr.id, tr );
		rowDataMap.write( tr.data.toString(), tr );
		// update the TableIndex
		tableIndex.write( tr );
	}

	// Pull a new row id
	private String nextRowId () {
		// increment the highestRowId
		highestRowId = highestRowId.add( BigInteger.ONE );
		// return the new id
		if ( rowIdMap.defined(highestRowId.toString()) ) {
			// recursion
			return nextRowId();
		} else {
			return highestRowId.toString();
		}
	}
	
	// check the highestRowId
	private void checkRowId( String id ) {
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
	
	// map keys[] -> values[]
	private StringMap1D<String> twoArraysMap (String[] keys, String[] values) {
		StringMap1D<String> aMap = new StringMap1D<>();
		for (int i = 0; i < keys.length; i++) {
			if (values!=null && values.length > i && values[i] != null) {
				aMap.write( keys[i], values[i] );
			} else {
				aMap.write( keys[i], "" );
			}
		}
		return aMap;
	}
	
	// map keys[] -> values[]
	private StringMap1D<String> blankMap (String[] keys) {
		StringMap1D<String> aMap = new StringMap1D<>();
		for (int i = 0; i < keys.length; i++) {
			aMap.write( keys[i], "" );
		}
		return aMap;
	}
	
	// Remove first element from an array
	private String[] removeFirst (String[] a) {
		if (a.length > 1) {
			return Arrays.copyOfRange(a, 1, a.length);
		} else {
			return new String[]{};
		}
	}
	
	// encode (embedded commas)
	private String[] encodeArray (String[] a) {
		for (int i=0; i<a.length; i++) {
			a[i] = a[i].replace( ",", "%2C" );
		}
		return a;
	}
	
	// decode (embedded commas)
	private String[] decodeLine (String line) {
		String[] a = line.replace("\r","").replace("\n","").split(",");
		for (int i=0; i<a.length; i++) {
			a[i] = a[i].replace( "%2C", "," );
		}
		return a;
	}
	
	// table memory usage
	void printMemoryUsage () {
		
		int mb = 1024*1024;
		
		//Getting the runtime reference from system
		Runtime runtime = Runtime.getRuntime();
		
		System.out.println("##### Heap utilization statistics [MB] #####");
		
		//Print used memory
		System.out.println("Used Memory:" 
			+ (runtime.totalMemory() - runtime.freeMemory()) / mb);

		//Print free memory
		System.out.println("Free Memory:" 
			+ runtime.freeMemory() / mb);
		
		//Print total available memory
		System.out.println("Total Memory:" + runtime.totalMemory() / mb);

		//Print Maximum available memory
		System.out.println("Max Memory:" + runtime.maxMemory() / mb);

	}
}
