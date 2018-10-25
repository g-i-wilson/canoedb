package io.github.gabrielwilson3.canoedb;

import java.util.*;
import java.net.*;
import java.math.BigInteger;

public class Query {
	
	// native output format is rowMap
	// rowMap: row_str -> table -> column -> data
	StringMap3D<String> rowMap = new StringMap3D<>();
	// colMap: table -> column -> data -> quantity
	StringMap3D<String> colMap;
	
	// input
	// table -> column -> data
	StringMap2D<String> inputTemplate = new StringMap2D<>();
	// output
	// table -> column -> null
	StringMap2D<String> outputTemplate = new StringMap2D<>();
		
	// database object
	Database db;	
	
	// DEFAULT SETTINGS
	// output map (just a reference to either the rowMap or colMap)
	StringMap3D<String> outputMap = rowMap;
	// flags
	boolean writeMode 	= false;
	boolean hasExecuted	= false;
	boolean ANDlogic 	= true; // inclusive (OR) or exclusive (AND) -- default is AND
	
	
	// Constructor
	public Query (Database d) {
		db = d;
	}
	
	// Add an output
	public Query output (String table, String column) {
		outputTemplate.write( table, column, null );
		return this;
	}
	
	// Add an input
	public Query input (String table, String column, String data) {
		inputTemplate.write( table, column, data );
		return this;
	}
	
	// Execute read or write-read
	public Query execute() {
		System.out.println( "\n\nQuery: input: "+inputTemplate );
		System.out.println( "\nQuery: output: "+outputTemplate+"\n\n" );
		// write (only once per Query)
		if (!hasExecuted && writeMode) {
			//
			// ************* write code ****************
			//
		}
		// read
		for (String table : inputTemplate.keys()) {
			for (String column : inputTemplate.keys(table)) {
				String data = inputTemplate.read(table, column);
				for (TableRow tr : db.table(table).search(column, data)) {
					StringMap2D<String> output = outputTemplate.cloned();
					StringMap2D<String> input = ( ANDlogic ? inputTemplate.cloned() : null );
					if ( tr.read( input, output ) ){
						// use the "stringified" version of a output HashMap as its key (eliminates any duplicate rows)
						rowMap.write( output.toString(), output.map() );
						System.out.println( "Query: row: "+tr );
					}
				}
				// we only need the first filter (i.e. first set of "stem" rows) under AND logic
				if (ANDlogic) break;
			}
		}
		hasExecuted = true;
		return this;
	}
	
	// set AND logic
	public Query and() {
		ANDlogic = true;
		return this;
	}
	
	// set OR logic
	public Query or() {
		ANDlogic = false;
		return this;
	}
	
	
	// set Database
	public Query database(Database d) {
		db = d;
		return this;
	}
	
	// set mode to write-read
	public Query write() {
		// if we're switching to write and have already executed, then re-execute
		if(!writeMode && hasExecuted) {
			writeMode = true;
			execute();
		} else writeMode = true;
		return this;
	}
	
	// set mode to read-only
	public Query read() {
		writeMode = false;
		return this;
	}
	
	// set map to rowMap
	public Query rows() {
		// all the data is already in rowMap
		outputMap = rowMap;
		return this;
	}
	
	// set map to colMap, and re-map data
	public Query columns() {
		colMap = new StringMap3D<>();
		// make sure we've executed first
		if(!hasExecuted) execute();
		// map data from rowMap into colMap
		for ( String row : rowMap.keys() ) {
			for ( String table : rowMap.keys(row) ) {
				for ( String column : rowMap.keys(row, table) ) {
					String rowData = rowMap.read(row, table, column);
					String colData = colMap.read(table, column, rowData);
					colMap.write(table, column, rowData, incrementNumericalString(colData) );
				}
			}
		}
		// route output from colMap
		outputMap = colMap;
		System.out.println( "Query: rowMap: "+rowMap );
		System.out.println( "Query: colMap: "+colMap );
		System.out.println( "Query: outputMap: "+outputMap );
		return this;
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
	
	// Create a JSON string from one of the Map objects
	public String outputJSON () {
		// make sure we've executed first
		if(!hasExecuted) execute();
		
		String output = "{\n";
		String a_comma = "\n";
		for ( String a : outputMap.keys() ) {
			output += a_comma+"\t\""+a+"\" : {";
			a_comma = ",\n";
			String b_comma = "\n";
			for ( String b : outputMap.keys(a) ) {
				output += b_comma+"\t\t\""+b+"\" : {";
				b_comma = ",\n";
				String c_comma = "\n";
				for ( String c : outputMap.keys(a,b) ) {
					try {
						output += c_comma+"\t\t\t\""+c+"\" : \""+outputMap.read(a,b,c).replace("\"","\\\"")+"\"";
						c_comma = ",\n";
						System.out.println( "Query: added data point "+outputMap.read(a,b,c) );
					} catch (Exception e) {
						System.out.println( "Query: problem with data point '"+outputMap.read(a,b,c)+"'" );
						e.printStackTrace(System.out);
					}
				}
				output += "\n\t\t}";
			}
			output += "\n\t}";
		}
		return output+"\n}";
	}

	// Create a CSV string from one of the Map objects
	public Query outputCSV () {
		// make sure we've executed first
		if(!hasExecuted) execute();

		return this;
	}

	// Parse the query string as CGI key=value&key=value tuples
	public Query parse (String query) {
		// loop through cgi data query and directly map input, output, and operation data
		String[] tuples = query.split("&");
		for (int i=0;i<tuples.length;i++) {
			String[] tuple = tuples[i].split("=");
			try {
				String[] table_column = URLDecoder.decode(tuple[0]).split("\\.");
				String data = ( tuple.length>1 ? URLDecoder.decode(tuple[1]) : "" );
				String table = table_column[0];
				String column = table_column[1];
				if (data.equals("")) {
					// empty data string is a place-holder for an output
					this.output(table, column);
					System.out.println("Query output: table="+table+", column="+column);
				} else {
					// data that is not empty is considered an input
					this.input(table, column, data);
					System.out.println("Query input: table="+table+", column="+column+", data="+data);
				}
			} catch(Exception e) {
				System.out.println("Didn't understand tuple: "+tuples[i]);
				//e.printStackTrace(System.out);
			}
		}
		return this;
	}	

	
	
}
