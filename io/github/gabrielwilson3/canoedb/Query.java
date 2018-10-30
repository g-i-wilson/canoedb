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
	
	// output String
	String output = "";
	// MIME format
	String mime = "text/html";
	
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
						rowMap.write( output.map.toString(), output.map );
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
	
	// set Database
	public Query database(Database d) {
		db = d;
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
	
	// Create a JSON string from the outputMap
	public String outputJSON () {
		// make sure we've executed first
		if(!hasExecuted) execute();
		return outputMap.toJSON();
	}

	// Create a CSV string from the outputMap
	public String outputCSV () {
		// make sure we've executed first
		if(!hasExecuted) execute();
		String csv = "";
		return csv;
	}

	// Generate interactive form HTML from the outputMap
	public String outputForm () {
		System.out.println("Query: outputing form HTML...");
		// make sure we've executed first
		if(!hasExecuted) execute();
		System.out.println( "Query: outputMap: "+outputMap );
		// start HTML and start the form table
		String html = "<html>\n<head>\n<title>CanoeDB</title>\n</head>\n<body>\n<form>\n<table>\n";
		// loop through all the tables and columns
		for (String table : db.tables()) {
			for (String column : db.table(table).columns()) {
				html += "<tr><td>"+table+"."+column+"<input name=\""+table+"."+column+"\" width=20></tr></td>\n";
			}
		}
		// complete the form table and start the output table
		html += "</table>\n<input type=\"submit\"></form>\n<br>\n<br>\n<table>\n<tr>";
		// table headers
		for (String row : outputMap.keys()) {
			for (String table : outputMap.keys(row)) {
				for (String column : outputMap.keys(row, table)) {
					String colText = table+"."+column;
					html += "<th>"+colText+"</th>\n";
				}
			}
			break;
		}
		html += "</tr>\n";
		// table rows
		for (String row : outputMap.keys()) {
			html += "<tr>\n";
			for (String table : outputMap.keys(row)) {
				for (String column : outputMap.keys(row, table)) {
					html += "<td>"+outputMap.read(row, table, column)+"</td>\n";
				}
			}
			html += "</tr>\n";
		}
		// complete the output table and complete HTML
		html += "</table>\n</body>\n</html>\n";
		return html;
	}
	
	// Set the output mode for this query
	public Query command (String c) {
		System.out.println("Query: command \""+c+"\"");
		switch (c) {
			case "and" :
				ANDlogic = true;
				break;
			case "or" :
				ANDlogic = false;
				break;
			case "write" :
				// if we're switching to write and have already executed, then re-execute
				if(!writeMode && hasExecuted) {
					writeMode = true;
					execute();
				} else writeMode = true;
				break;
			case "read" :
				writeMode = false;
				break;
			case "columns" :
				outputMap = colMap;
				// load the colMap
				columns();
				break;
			case "rows" :
				outputMap = rowMap;
				break;
			case "json" :
				output = outputJSON();
				mime = "application/json";
				break;
			case "csv" :
				output = outputCSV();
				mime = "text/csv";
				break;
			case "form" :
				output = outputForm();
				mime = "text/html";
				break;
		}
		return this;
	}

	// Get the output String from this query
	public String output () {
		if(!hasExecuted) {
			execute();
			output = outputForm();
		}
		return output;
	}
	
	// Get the MIME format
	public String mime () {
		return mime;
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
				System.out.println("Query: didn't understand tuple: "+tuples[i]);
				//e.printStackTrace(System.out);
			}
		}
		return this;
	}	

	
	
}
