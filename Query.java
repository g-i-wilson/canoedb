package canoedb;

import java.util.*;
import java.net.*;
import java.math.BigInteger;
import canoedb.transforms.*;

public class Query {

	// Database.execute(this) populates rowMap
	// rowMap: row_str -> table -> column -> data
	StringMap3D<String> rowMap = new StringMap3D<>();
	// colMap: table -> column -> data -> quantity
	StringMap3D<String> colMap = new StringMap3D<>();
	// structMap: table -> column -> reference||transform -> table_name||transform_name
	StringMap3D<String> structMap = new StringMap3D<>();

	// input
	// table -> column -> data
	StringMap2D<String> inputTemplate = new StringMap2D<>();
	// output
	// table -> column -> data
	StringMap2D<String> outputTemplate = new StringMap2D<>();
	// transform
	// table -> column -> transformObject
	StringMap2D<String> transformNames = new StringMap2D<>();
	StringMap2D<Transform> transformMap = new StringMap2D<>();

	// peak list (read)
	List<TableRow> readOrigins = new ArrayList<>();

	// peak (or partial-summit) list (write)
	List<Table> writeOrigins = new ArrayList<>();

	// database object
	Database db;

	// output map (just a reference to either the rowMap or colMap)
	StringMap3D<String> outputMap = rowMap;

	// Query properties
	int 		sessionId;
	boolean 	write = false;
	boolean		nullsAllowed = false;
	boolean		zeroLengthFiltersEnabled = false;
	String 		logic = "and";

	// Query timing and messages
	long		intervalTime;
	long		startTime;
	String		logText = "";

	// Query general
	private boolean columnsMapped = false;


	// Constructor
	public Query (Database d, int id) {
		// set Database object
		db = d;
		// set sessionId
		sessionId = id;
		// start time
		startTime = System.nanoTime();
		intervalTime = startTime;
	}

	// Add an output
	public Query output (String table, String column) {
		outputTemplate.vivify( table, column );
		if (zeroLengthFiltersEnabled) inputTemplate.write( table, column, "" );
		return this;
	}

	// Add an input
	public Query input (String table, String column, String data) {
		inputTemplate.write( table, column, data );
		outputTemplate.vivify( table, column );
		return this;
	}

	// Read an input
	public String input (String table, String column) {
		return inputTemplate.read( table, column );
	}

	// Read inputs
	public StringMap2D<String> input () {
		return inputTemplate;
	}

	// Add a transform
	public Query transform (String table, String column, String tranName) {
		transformNames.write( table, column, tranName );
		transformMap.write( table, column, db.transform( tranName ) );
		return this;
	}

	// Get the database
	public Database db () {
		return db;
	}

	// Get the structure of the Database
	public StringMap3D<String> structure () {
		return structMap;
	}

	// Get a StringMap3D with results sorted by rows
	public StringMap3D<String> rows () {
		return rowMap;
	}

	// Get a StringMap3D with results sorted by columns
	public StringMap3D<String> columns () {
		if (! columnsMapped) mapToColumns();
		return colMap;
	}

	// Filtered set of TableRows from a Table (specifiy filter)
	public Collection<TableRow> rows ( Table t, String column, String filter ) {
		if (transformMap.defined( t.name, column )) {
			log("Query: using QUERY Transform '"+transformNames.read( t.name, column )+"' with filter '"+filter+"' applied to column '"+t.name+"."+column+"'");
			return transformMap.read( t.name, column ).tableRows( t, column, filter );
		} else if (t.transformMap.defined(column)) {
			log("Query: using TABLE Transform '"+transformNames.read( t.name, column )+"' with filter '"+filter+"' applied to column '"+t.name+"."+column+"'");
			return t.transformMap.read( column ).tableRows( t, column, filter );
		} else {
			log("Query: using pass-thru Transform with filter '"+filter+"' applied to column '"+t.name+"."+column+"'");
			return t.null_transform.tableRows( t, column, filter );
		}
	}
	// Filtered set of TableRows from a Table (automatically use the filter in inputTemplate)
	public Collection<TableRow> rows ( Table t, String column ) {
		String filter = inputTemplate.read( t.name, column );
		if (filter!=null) {
			return rows( t, column, filter );
		} else {
			log("Query: NULL found in q.inputTemplate for '"+t.name+"."+column+"', so returning an empty set of rows");
			return t.null_collection;
		}
	}

	// increment a numerical value held as a String
	private String incrementNumericalString (String numberString) {
		try {
			BigInteger bigNum = new BigInteger(numberString);
			String numberPlusOne = bigNum.add(BigInteger.ONE).toString();
			return numberPlusOne;
		} catch (Exception e) {
			return "1";
		}
	}

	// map data from rowMap into colMap
	void mapToColumns() {
		// map data from rowMap into colMap
		for ( String row : rowMap.keys() ) {
			for ( String table : rowMap.keys(row) ) {
				for ( String column : rowMap.keys(row, table) ) {
					String rowData = rowMap.read(row, table, column);
					String colData = colMap.read(table, column, rowData);
					colMap.write( table, column, rowData, incrementNumericalString(colMap.read(table,column,rowData)) );
				}
			}
		}
		columnsMapped = true;
	}

	// structure of the database
	void databaseStructure () {
		for (String table : db.tables()) {
			Table t = db.table(table);
			for (String column : t.columns()) {
				structMap.vivify( table, column ); // at least vivify the table and column.
				// if there's a reference or transform, then add that property
				if (!t.referenceNames.read(column).equals("")) structMap.write( table, column, "reference", t.referenceNames.read(column) );
				if (!t.transformNames.read(column).equals("")) structMap.write( table, column, "transform", t.transformNames.read(column) );
			}
		}
	}

	// Execute with defaults
	public Query execute () {
		return execute( "" );
	}

	// Execute with write and others default
	public Query execute ( boolean write ) {
		return execute( "", write, logic, nullsAllowed, zeroLengthFiltersEnabled );
	}

	// Execute with data and others default
	public Query execute ( String data ) {
		return execute( data, write, logic, nullsAllowed, zeroLengthFiltersEnabled );
	}

	// Execute with data, write, and others default
	public Query execute ( String data, boolean write ) {
		return execute( data, write, logic, nullsAllowed, zeroLengthFiltersEnabled );
	}

	// Execute Query with all config options
	public Query execute ( String data, boolean w, String l, boolean n, boolean z ) {
		// Query settings
		write = w;
		logic = l;
		nullsAllowed = n;
		zeroLengthFiltersEnabled = z;
		// Query key-value pairs
		log( "Query: parsing data..." );
		parse( data );
		// log filters (input) and columns (output)
		log( "Query: input: "+inputTemplate );
		log( "Query: output: "+outputTemplate );
		// begin execute
		log("Executing query...");
		db.execute( this );
		log("Finished executing query");
		// map data from the rows structure to the columns structure
		mapToColumns();
		// refresh the structure of the database
		databaseStructure();
		return this;
	}

	// Parse the query string as CGI key=value&key=value tuples
	public Query parse (String query) {
		// loop through cgi data query and directly map input, output, and operation data
		log("Query: parsing '"+query+"'");
		String[] tuples = query.split("&");
		for (int i=0;i<tuples.length;i++) {
			String[] tuple = tuples[i].split("=");
			try {
				String[] table_column_transform = URLDecoder.decode(tuple[0]).split("\\.");
				String data = ( tuple.length>1 ? URLDecoder.decode(tuple[1]) : "" );
				String table = table_column_transform[0];
				String column = table_column_transform[1];
				String tranName = ( table_column_transform.length>2 ? table_column_transform[2] : null );
				if (data.equals("")) {
					// empty data string is a place-holder for an output
					output(table, column);
				} else {
					// data that is not empty is considered an input
					input(table, column, data);
				}
				if (tranName!=null) {
					transform(table, column, tranName);
				}
			} catch(Exception e) {
				log("Query: didn't understand tuple: "+tuples[i]);
			}
		}
		return this;
	}

	// log message and time elapsed
	public void log () {
		log("");
	}
	public void log ( String s ) {
		long currentTime = System.nanoTime();
		long usCurrent = (currentTime - startTime)/1000;
		long usInterval = (currentTime - intervalTime)/1000;
		intervalTime = currentTime;
		logText += "["+sessionId+"] [total:"+usCurrent+"us, delta:"+usInterval+"us] "+s+"\n";
	}

	public String toString () {
		return logText;
	}

}
