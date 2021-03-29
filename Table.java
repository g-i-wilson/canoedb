package canoedb;

import java.util.*;
import java.math.BigInteger;
import java.nio.file.*;
import java.io.File;
import java.lang.*;
import canoedb.transforms.*;


public class Table {

	// Table properties
	String 					name;
	StringMap1D<String>		columnNames = new StringMap1D<>();
	StringMap1D<String> 	referenceNames = new StringMap1D<>();
	StringMap1D<String> 	transformNames = new StringMap1D<>();
	StringMap1D<Transform> 	transformMap = new StringMap1D<>();

	// Table rows
	StringMap1D<TableRow> 	rowIdMap = new StringMap1D<>();
	StringMap1D<TableRow> 	rowDataMap = new StringMap1D<>();
	BigInteger 				highestRowId = new BigInteger("0");

	// Table file
	File					tableFile;

	// Table index
	public TableIndex 		tableIndex = new TableIndex( this );

	// file exists
	boolean					fileExists = false;

	// downhill
	StringMap1D<Table>		toMap = new StringMap1D<>(); // tableName -> table immediately downhill from this table
	Set<String>				toSet = new HashSet<>(); // set of all tables reachable downhill from this table
	// uphill
	List<Table>				fromList = new ArrayList<>(); // table(s) immediately uphill from this table

	// null objects
	StringMap1D<TableRow> 	null_map = new StringMap1D<>();
	Set<String>				null_set = new LinkedHashSet<>();
	Transform				null_transform = new Transform();
	Collection<TableRow> 	null_collection = new ArrayList<>();

	// load onRead & onWrite classes
	ClassLoader				classLoader = TableRow.class.getClassLoader();



	// set the physical path to the Table
	Table ( String filePath ) {
		tableFile = new File( filePath );
		initToMemory();
	}
	Table ( File f ) {
		tableFile = f;
		initToMemory();
	}
	// initialize Table in memory
	void initToMemory () {
		String fileName = tableFile.getName();
		int pos = fileName.lastIndexOf(".");
		if (fileName.lastIndexOf(".") > 0) {
			fileName = fileName.substring(0, pos);
		}
		name = fileName;
		System.out.println(this+": initialized "+name);
	}

	// Load table from a physical file
	boolean read () {
		// read CSV file
		try (Scanner sc = new Scanner(tableFile, "UTF-8")) {
			String[] columns = removeFirst( decodeLine(sc.nextLine()) );
			// initialize the basic table info maps
			columnNames		= blankMap( columns );
			referenceNames	= twoArraysMap( columns, removeFirst(decodeLine(sc.nextLine())) );
			transformNames = twoArraysMap( columns, removeFirst(decodeLine(sc.nextLine())) );
			System.out.println(this+": "+columnNames);
			fileExists = true;
			// Load the Transform objects
			for (String column : columnNames.keys()) {
				if (!transformNames.read(column).equals("")) {
					String binName = "canoedb.transforms."+transformNames.read(column);
					try {
						Class aClass = classLoader.loadClass(binName);
						Object anObject = aClass.newInstance();
						Transform transformObject = (Transform) anObject;
						transformMap.write( column, transformObject );
						System.out.println(this+": loaded Transform object '"+binName+"'");
					} catch (Exception e) {
						System.out.println(this+": ERROR: unable to load Transform object '"+binName+"'");
						e.printStackTrace();
					}
				}
			}
			// Loop through lines and fill fileMap with TableRow objects
			while (sc.hasNextLine()) {
				// read a CSV data line: id,data1,dataN
				String[] 	data 	= decodeLine( sc.nextLine() );
				String 		rowId 	= data[0];
				String[] 	rowData = removeFirst( data );
				// spawn a TableRow
				TableRow tr = new TableRow( this, rowId, twoArraysMap( columns, rowData ) );
				//System.out.println(this+": read row "+tr.data.toString());
				checkRowId( tr.id );
				logTableRow( tr );
				//System.out.println(tr);
			}
			// Scanner suppresses io exceptions
			if (sc.ioException() != null) System.out.println( this+": file io exception: "+sc.ioException() );
		} catch (Exception e) {
			System.out.println( this+": file Scanner exception: "+e );
			e.printStackTrace();
			return false;
		}
		// memory
		printMemoryUsage();
		// able to read file
		return true;
	}

	// get a Set of all ID Strings (corresponding to TableRow objects) in this Table
	public Set<String> rows () {
		return rowIdMap.keys();
	}

	// get the last TableRow in the table
	public TableRow last () {
		return row(
			(String) rows().toArray()[ rows().size()-1 ]
		);
	}

	// Creates a new table row (virtual; not yet appended)
	public TableRow row () {
		// spawn a new row
		TableRow tr = new TableRow(this, nextRowId(), columnNames.cloned());
		System.out.println( this+" '"+name+"': added row '"+tr.id+"' (auto-ID)" );
		return tr;
	}

	// Get a TableRow by ID or create an empty TableRow for that ID
	public TableRow row ( String id ) {
		if (rowIdMap.defined(id)) {
			// access a real row
			return rowIdMap.read(id);
		} else {
			// spawn a new row (virtual; not yet appended)
			TableRow tr = new TableRow(this, id, columnNames.cloned());
			System.out.println( this+" '"+name+"': added row '"+tr.id+"'" );
			logTableRow( tr );
			return tr;
		}
	}

	// Get a TableRow by data
	public TableRow row ( StringMap1D<String> data ) {
		if (rowDataMap.defined(data.toString())) {
			return rowDataMap.read(data.toString());
		} else {
			TableRow tr = row();
			tr.write( data );
			append( tr );
			checkRowId( tr.id );
			logTableRow( tr );
			return tr;
		}
	}

	// initialize Table to disk
	boolean initToDisk ( Query q ) {
		// update Table in memory based on the Query
		columnNames = q.inputTemplate.cloned(name);
		transformNames = q.transformNames.cloned(name);
		transformMap = q.transformMap.cloned(name);
		for (String tableName : q.inputTemplate.keys()) {
			if (tableName.equals(name)) continue; // skip this table
			String newColumn = tableName+" (reference column)"; // create a reference to all the others
			columnNames.write( newColumn, "" );
			referenceNames.write( newColumn, tableName );
		}
		// create a file header string
		String str = "";
		for (String column : columnNames.keys())
			str += ","+column;
		str += "\n";
		for (String column : columnNames.keys())
			str += ","+( referenceNames.defined(column) ? referenceNames.read(column) : "" );
		str += "\n";
		for (String column : columnNames.keys())
			str += ","+( transformNames.defined(column) ? transformNames.read(column) : "" );
		// write the Table to disk
		try {
			Files.write(tableFile.toPath(), str.getBytes());
			fileExists = true;
			System.out.println( this+" '"+name+"': written to file '"+tableFile+"'" );
		} catch (Exception e) {
			System.out.println(this+": ERROR writing to file '"+tableFile+"'");
			System.out.println(e);
			e.printStackTrace();
			return false;
		}
		return true;
	}

	// append to the end of the Table file
	public boolean append (TableRow tr) {
		// append the TableRow to the Table file
		String str = "\n"+tr.id+","+tr.data.join(",");
		try {
			// attempt to append to the file
			Files.write(tableFile.toPath(), str.getBytes(), StandardOpenOption.APPEND);
			// make sure the TableRow has been logged
			logTableRow( tr );
			// record in the index
			tableIndex.write( tr );
			System.out.println( this+" '"+name+"': appended row '"+tr.id+"'" );
		} catch (Exception e) {
			System.out.println(this+": ERROR appending to file '"+tableFile+"'");
			System.out.println(e);
			e.printStackTrace();
			return false;
		}
		return true;
	}

	// columns in Table
	public Set<String> columns () {
		return columnNames.keys();
	}

	// link TO Table
	void linkTo ( String column, Table t ) {
		toMap.write(column, t);
	}
	// link FROM Table
	boolean linkFrom ( Table t ) {
		if (!fromList.contains(t)) {
			fromList.add( t );
			return true;
		} else return false;
	}

	// SET DOWNHILL (START): all tables reachable downhill from this table
	void downhill () {
		// grab this Table's toSet, and use that in the recursive continueDownhill function
		toSet.add( name ); // this table
		continueDownhill( toSet );
		System.out.println(this+": "+name+": toSet: "+toSet);
	}
	// SET DOWNHILL: continuation function for downhill function
	void continueDownhill (Set<String> someSet) {
		for (Table t : toMap.values()) {
			if (someSet.contains( t.name )) continue; // skip if alread in the set -- no infinite table reference loops allowed
			someSet.add( t.name );
			t.continueDownhill( someSet );
		}
	}

	// WRITE UPHILL traverse (recursive)
	void writeTraverse ( Query q ) {
		// if we're high enough up the mountain (toward any of the peaks) to have all tables (containsAll)
		if (toSet.containsAll( q.inputTemplate.keys() )) {
			q.log("SET: "+toSet);
			// start traversing downhill from this peak
			List<Table> tablesTraversed = new ArrayList<>();
			// make sure this is a new "origin" to start downhill from
			if (q.writeOrigins.contains(this)) return;
			q.writeOrigins.add(this);
			// create the peak table row and kick-off the downhill traversal
			q.log( this+": * PEAK: "+name );
			writeTraverseCont( tablesTraversed, q );
		} else {
			// continue trekking uphill
			for (Table t : fromList) {
				q.log( this+": / UPHILL: '"+name+"' -> '"+t.name+"'" );
				t.writeTraverse( q );
			}
		}
	}

	// WRITE DOWNHILL traverse (recursive)
	private TableRow writeTraverseCont (
		List<Table> tablesTraversed,
		Query q
	) {
		// no infinite table loops allowed
		if (tablesTraversed.contains(this)) return null;
		tablesTraversed.add(this);

		// create a blank TableRow object
		TableRow tr = row();

		// loop through the columns and fill in with data or reference strings
		for ( String column : tr.data.keys() ) {
			q.log( this+" "+name+":"+tr.id+": column "+column );
			if (q.inputTemplate.defined(name, column)) {
				tr.write( column, q.inputTemplate.read(name, column), q.transformMap.read(name, column) );
				q.log(this+" "+name+": updated '"+column+"' of '"+tr+"' with '"+q.inputTemplate.read(name, column)+"'");
			} else if (toMap.defined(column)) {
				Table t = toMap.read(column);
				q.log( this+": \\ DOWNHILL: '"+name+"' -> '"+t.name+"'" );
				TableRow other_tr = t.writeTraverseCont(tablesTraversed, q);
				if (other_tr!=null) {
					tr.write( column, other_tr.id );
					tr.linkTo( other_tr );
					q.log(this+" "+name+": added TableRow reference in '"+tr+"' under '"+column+"' to '"+other_tr+"'");
					// just link to; doesn't affect the other_tr TableRow yet...
				}
			}
		}

		// check to see if any data (or table references) have actually been added, and return null otherwise
		if (tr.data.allNulls()) {
			q.log(this+" "+name+": new TableRow has allNulls(), so returning null");
			return null;
		}

		// verify that another similar TableRow doesn't already exist, and if so, use the old
		String hash = tr.hash();
		if (rowDataMap.defined(hash)) {
			TableRow tr_old = rowDataMap.read(hash);
			q.log(this+" "+name+": similar TableRow already exists; using row '"+tr_old+"'");
			// copy the links from the new TableRow over to the old TableRow
			for ( TableRow linked_tr : tr.to ) {
				tr_old.linkTo( linked_tr );
				linked_tr.linkFrom( tr_old );
			}
			// replace the new TableRow reference with a reference to the old TableRow
			tr = tr_old;
		} else {
			for ( TableRow linked_tr : tr.to ) {
				tr.linkTo( linked_tr );
				linked_tr.linkFrom( tr );
			}
			// add the new TableRow to the Table file
			append( tr );
		}

		// return the new (or old) TableRow
		return tr;
	}


	// referenced Table
	public String reference (String column) {
		if (referenceNames!=null && referenceNames.defined(column))
			return referenceNames.read(column);
		else return "";
	}

	// tranform Class
	public Transform transform (String column) {
		if (transformMap.defined(column)) {
			return transformMap.read(column);
		} else return null_transform;
	}

	// log a TableRow in the ID & data maps, and also the index
	private void logTableRow ( TableRow tr ) {
		rowIdMap.write( tr.id, tr );
		rowDataMap.write( tr.hash(), tr );
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
		//System.out.println( id+", "+highestRowId.toString() );

	}

	// map keys[] -> values[]
	private StringMap1D<String> twoArraysMap (String[] keys, String[] values) {
		StringMap1D<String> aMap = new StringMap1D<>();
		for (int i = 0; i < keys.length; i++) {
			if (values!=null && values.length > i && values[i] != null) {
				aMap.write( new String(keys[i]), values[i] );
			} else {
				aMap.write( new String(keys[i]), "" );
			}
		}
		return aMap;
	}

	// Create a blank StringMap1D
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

		System.out.println(this+": heap utilization after loading table '"+name+"'");

		//Print used memory
		System.out.println(this+": memory used: "+( (runtime.totalMemory() - runtime.freeMemory()) / mb )+"MB");

		//Print free memory
		System.out.println(this+": memory free: "+( runtime.freeMemory() / mb )+"MB");

		//Print total available memory
		System.out.println(this+": memory total: "+( runtime.totalMemory() / mb )+"MB");

		//Print Maximum available memory
		System.out.println(this+": memory max: "+( runtime.maxMemory() / mb )+"MB");

	}
}
