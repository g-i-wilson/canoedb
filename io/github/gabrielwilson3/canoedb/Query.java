package io.github.gabrielwilson3.canoedb;

import java.util.*;
import java.net.*;
import java.math.BigInteger;
import io.github.gabrielwilson3.canoedb.transforms.*;

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
	int			sessionId;
	String		output 		= "";
	boolean 	write 		= false;
	String 		logic 		= "and";
	String		format		= "form";
	String 		mime 		= "text/html; charset=utf-8";
	
	// Query timing and messages
	long		intervalTime;
	long		startTime;
	String		logText = "";
	
	
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
		return this;
	}
	
	// Add an input
	public Query input (String table, String column, String data) {
		inputTemplate.write( table, column, data );
		outputTemplate.vivify( table, column );
		return this;
	}
	
	// Add a transform
	public Query transform (String table, String column, String tranName) {
		transformNames.write( table, column, tranName );
		transformMap.write( table, column, db.transform( tranName ) );
		return this;
	}
	
	// Filtered set of TableRows from a Table (specifiy filter)
	public Collection<TableRow> rows ( Table t, String column, String filter ) {
		if (transformMap.defined( t.name, column )) {
			log("Query: using CUSTOM Transform '"+transformNames.read( t.name, column )+"' with filter '"+filter+"', to find TableRows in "+t.name);
			return transformMap.read( t.name, column ).tableRows( t, column, filter );
		} else {
			log("Query: using DEFAULT Transform '"+t.transformNames.read( column )+"' with filter '"+filter+"', to find TableRows in "+t.name);
			return t.null_transform.tableRows( t, column, filter );
		}
	}
	// Filtered set of TableRows from a Table (automatically use the filter in inputTemplate)
	public Collection<TableRow> rows ( Table t, String column ) {
		String filter = inputTemplate.read( t.name, column );
		if (filter!=null) {
			return rows( t, column, filter );
		} else {
			log("Query: null set of TableRows from table "+t.name);
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
	}
	
	// refresh the structure of the database
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

	// Create a JSON string from the outputMap
	String outputJSON () {
		log("Query: generating JSON...");
		return	"{\n"+
				"\"structure\" : "+
				structMap.toJSON()+
				",\n"+
				"\"columns\" : "+
				colMap.toJSON()+
				",\n"+
				"\"rows\" : "+
				rowMap.toJSON()+
				"\n}";
	}

	// Create a CSV string from the outputMap
	String outputCSV () {
		log("Query: generating CSV...");
		String csv = "";
		return csv;
	}
	
	// Generate interactive form HTML from the outputMap
	String outputForm () {
		log("Query: generating form HTML...");
		// start HTML and start the form table
		String html = 
			"<html>\n<head>\n<title>CanoeDB</title>\n<style>\n"+
			"body { font-family:sans-serif; }"+
			"div { width:100%; overflow-x:auto; overflow-y:hidden; }\n"+
			"table { border-collapse:collapse; table-layout:fixed; }\n"+
			"th, td { padding:10px; text-align:left; }\n"+
			"</style></head>\n<body>\n<div>\n<form id=\"main_form\" method=\"post\">\n<table>\n<tr>\n";
		// loop through all the tables and columns
		for (String table : db.tables()) {
			Table t = db.table(table);
			for (String column : t.columns()) {
				if (t.reference(column).equals("")) {
					html +=
						"<td>"+table+"<br>"+column+"<br>"+
						"<input name=\""+table+"."+column+"\" list=\""+table+"."+column+"_list\" "+
						"value=\""+(inputTemplate.read(table, column)!=null ? inputTemplate.read(table, column) : "")+"\" "+
						"onchange=\"document.getElementById('main_form').submit()\" "+
						"onblur=\"document.getElementById('main_form').submit()\" "+
						"onfocus=\"this.value=''\" "+
						"size=5>\n"+
						"</td>\n<datalist id=\""+table+"."+column+"_list\">\n";
					for (String data : colMap.keys(table, column)) {
						html += "<option value=\""+data+"\">\n";
					}
					html += "</datalist>\n";
				}
			}
		}
		// complete the form table and start the output table
		html += "</tr>\n</table>\n</form>\n</div>\n<br>\n<div>\n<table>\n<tr>";
		// table headers
		for (String row : outputMap.keys()) {
			for (String table : outputMap.keys(row)) {
				Table t = db.table(table);
				for (String column : outputMap.keys(row, table)) {
					if (t.reference(column).equals("")) {
						html += "<th>"+table+"<br>"+column+"</th>\n";
					}
				}
			}
			break;
		}
		html += "</tr>\n";
		// table rows
		for (String row : outputMap.keys()) {
			html += "<tr>\n";
			for (String table : outputMap.keys(row)) {
				Table t = db.table(table);
				for (String column : outputMap.keys(row, table)) {
					if (t.reference(column).equals("")) {
						String dataElement = outputMap.read(row, table, column);
						html += "<td>"+( dataElement!=null ? dataElement : "" )+"</td>\n";
					}
				}
			}
			html += "</tr>\n";
		}
		// complete the output table and complete HTML
		html += "</table>\n</div>\n</body>\n</html>\n";
		log("HTML complete");
		return html;
	}
	
	// Set the execution settings for this query
	public Query command (String c) {
		log("Query: command \""+c+"\"");
		switch (c) {
			case "and" :
				logic = "and";
				break;
			case "or" :
				logic = "or";
				break;
			case "xor" :
				logic = "xor";
				break;
			case "write" :
				write = true;
				break;
			case "read" :
				write = false;
				break;
			case "json" :
				format = "json";
				mime = "application/json; charset=utf-8";
				break;
			case "csv" :
				format = "csv";
				mime = "text/csv; charset=utf-8";
				break;
			case "form" :
				format = "form";
				mime = "text/html; charset=utf-8";
				break;
		}
		return this;
	}

	// Get the output String from this query
	public String output () {
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
		// return the query results using the data format requested
		switch(format) {
			case "json" :
				return outputJSON();
			case "csv" :
				return outputCSV();
			case "form" :
				return outputForm();
			default:
				return outputForm();
		}
	}
	
	// Get the MIME format
	public String mime () {
		return mime;
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
		logText += sessionId+"-> ["+usInterval+", "+usCurrent+"] "+s+"\n";
	}
	public String logString () {
		return logText;
	}
	
}
