package io.github.gabrielwilson3.canoedb;

import java.util.*;
import java.net.*;

public class Query {
	
	// output template
	// table -> column -> null
	Map<String, Map<String, String>> outputTemplate = new HashMap<> ();
	
	// native output format is rowMap
	// rowMap: row_str -> table -> column -> data
	Map<String, Map<String, Map<String, String>>> rowMap = new HashMap<>();
	// colMap: table -> column -> data -> quantity
	Map<String, Map<String, Map<String, String>>> colMap = new HashMap<>();
	
	// input
	// table -> column -> data
	Map<String, Map<String, String>> inputMap = new HashMap<> ();
	
	// database object
	Database dbObject;
	
	// logic: inclusive query (OR) or exclusive query (AND) -- default is AND
	boolean ANDlogic = true;
	
	// DEFAULT SETTINGS
	// output map (just a reference to either the rowMap or colMap)
	Map<String, Map<String, Map<String, String>>> outputMap = rowMap;
	// read-only mode (read) vs read-write mode (write)
	boolean writeMode = false;
	
	// flag to remember whether we've executed
	boolean hasExecuted = false;
	
	
	// Constructor when nothing is passed (e.g. extending this object)
	public Query () {}
	
	// Constructor when passed a Database object
	public Query (Database db) {
		dbObject = db;
	}
	
	// Add an output
	public Query output (String table, String column) {
		if (!outputTemplate.containsKey(table)) outputTemplate.put( table, new HashMap<String, String>() );
		outputTemplate.get( table ).put( column, null );
		return this;
	}
	
	// Add an input
	public Query input (String table, String column, String data) {
		if (!inputMap.containsKey(table)) inputMap.put( table, new HashMap<String, String>() );
		inputMap.get( table ).put( column, data );
		return this;
	}
	
	// loop through filters to get each TableRow
	// ask that TableRow to populate outputTemplate
	// add outputTemplate to rowMap
	public Query execute() {
		System.out.println( "\nQuery: rowMap: "+outputTemplate.toString()+"\n\n" );
		// loop through tables
		for (String table : inputMap.keySet()) {
			// loop through columns
			for (String column : inputMap.get(table).keySet()) {
				// get the data element
				String data = inputMap.get(table).get(column);
				// look up a table row(s) Map by data and column
				System.out.println( "Query: table: "+table+", column: "+column+", data: "+data );
				Table t = this.dbObject.tables(table);
				if (t != null) {
					Map<String, TableRow> trMap = t.index(column, data);
					System.out.println( "Query: stem table rows: "+trMap );
					for (TableRow trObject : trMap.values()) {
						// create a new output Map
						Map<String, Map<String, String>> output = this.cloneHash( this.outputTemplate );
						// create a new input Map (or null if using OR logic)
						Map<String, Map<String, String>> input = ( this.ANDlogic ? this.cloneHash( this.inputMap ) : null );
						// send input/output Maps to the tableRow object (output to be loaded by the tableRow)
						// getData returns true if it finds any data as it traverses the "virtual tableRow" via references
						if ( trObject.getData( input, output ) ){
							// use the "stringified" version of a output HashMap as its key (eliminates any duplicate rows)
							this.rowMap.put( "key<"+output.toString()+">", output );
							System.out.println( "Query: row: "+trObject.str() );
						}
					}
				}
				if (this.ANDlogic) break;
			}
		}
		this.hasExecuted = true;
		return this;
	}
	
	// set AND logic
	public Query and() {
		this.ANDlogic = true;
		return this;
	}
	
	// set OR logic
	public Query or() {
		this.ANDlogic = false;
		return this;
	}
	
	// Clone a new output map (used by execute)
	Map<String, Map<String, String>> cloneHash ( Map<String, Map<String, String>> template ) {
		Map<String, Map<String, String>> templateClone = new HashMap<>();
		// loop through (pseudo-code) table->Map<column,data>
		for ( String table : template.keySet() ) {
			Map<String, String> templateCloneColumn = new HashMap<>();
			// loop through (pseudo-code) column->data
			for ( String column : template.get(table).keySet() ) {
				templateCloneColumn.put( column, template.get(table).get(column) );
			}
			templateClone.put( table, templateCloneColumn );
		}
		return templateClone;
	}
	
	// set Database
	public Query database(Database db) {
		this.dbObject = db;
		return this;
	}
	
	// set map to rowMap
	public Map<String, Map<String, Map<String, String>>> map() {
		return this.outputMap;
	}
	
	// set mode to write-read
	public Query write() {
		// if we're switching to write and have already executed, then re-execute
		if(!this.writeMode && this.hasExecuted) {
			this.writeMode = true;
			this.execute();
		} else {
			this.writeMode = true;
		}
		return this;
	}
	
	// set mode to read-only
	public Query read() {
		this.writeMode = false;
		return this;
	}
	
	// set map to rowMap
	public Query rows() {
		// all the data is already in rowMap
		this.outputMap = this.rowMap;
		return this;
	}
	
	// set map to colMap, and re-map data
	public Query columns() {
		// make sure we've executed first
		if(!this.hasExecuted) this.execute();
		// map data from rowMap into colMap
		for ( String row : rowMap.keySet() ) {
			for ( String table : rowMap.get(row).keySet() ) {
				// auto-vivinate the table
				if (!colMap.containsKey(table)) colMap.put(table, new HashMap<String, Map<String, String>>());
				for ( String column : rowMap.get(row).get(table).keySet() ) {
					String data = rowMap.get(row).get(table).get(column);
					// auto-vivinate the column
					if (!colMap.get(table).containsKey(column)) colMap.get(table).put(column, new HashMap<String, String>());
					if (!colMap.get(table).get(column).containsKey(data)) {
						colMap.get(table).get(column).put(data, "1");
					} else {
						int data_qty = Integer.parseInt( colMap.get(table).get(column).get(data) ) + 1;
						colMap.get(table).get(column).put(data, Integer.toString(data_qty));
					}
				}
			}
		}
		System.out.println( "Query: rowMap="+this.rowMap.toString() );
		System.out.println( "Query: colMap="+this.colMap.toString() );
		System.out.println( "Query: outputMap="+this.outputMap.toString() );
		this.outputMap = this.colMap;
		System.out.println( "Query: outputMap="+this.outputMap.toString() );
		return this;
	}
	
	// Create a JSON string from one of the Map objects
	public String outputJSON () {
		// make sure we've executed first
		if(!this.hasExecuted) this.execute();

		String output = "{\n";
		String a_comma = "\n";
		for ( String a : outputMap.keySet() ) {
			output += a_comma+"\t\""+a+"\" : {";
			a_comma = ",\n";
			String b_comma = "\n";
			for ( String b : outputMap.get(a).keySet() ) {
				output += b_comma+"\t\t\""+b+"\" : {";
				b_comma = ",\n";
				String c_comma = "\n";
				for ( String c : outputMap.get(a).get(b).keySet() ) {
					try {
						output += c_comma+"\t\t\t\""+c+"\" : \""+outputMap.get(a).get(b).get(c).replace("\"","\\\"")+"\"";
						c_comma = ",\n";
						System.out.println( "Query: added data point "+outputMap.get(a).get(b).get(c) );
					} catch (Exception e) {
						System.out.println( "Query: problem with data point '"+outputMap.get(a).get(b).get(c)+"'" );
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
		if(!this.hasExecuted) this.execute();

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
