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
	// table -> column -> data
	StringMap2D<String> outputTemplate = new StringMap2D<>();
		
	// database object
	Database db;	
	
	// output String
	String output = "";
	// MIME format
	String mime = "text/html";
	
	// output map (just a reference to either the rowMap or colMap)
	StringMap3D<String> outputMap = rowMap;

	// Query properties
	boolean 	write 		= false;
	boolean 	executed	= false;
	String 		logic 		= "and";
	
	
	// Constructor
	public Query (Database d) {
		// set Database object
		db = d;
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
	
	// Execute read or write-read
	public Query execute() {
		System.out.println( "\n\nQuery: input: "+inputTemplate );
		System.out.println( "\nQuery: output: "+outputTemplate+"\n\n" );
		// read
		db.execute( this );
		executed = true;
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

	// set map to colMap, and re-map data
	public Query columns() {
		colMap = new StringMap3D<String>();
		// make sure we've executed first
		if(!executed) execute();
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
		//System.out.println( "Query: rowMap: "+rowMap );
		//System.out.println( "Query: colMap: "+colMap );
		//System.out.println( "Query: outputMap: "+outputMap );
		return this;
	}
	
	// set Database
	public Query database(Database d) {
		db = d;
		return this;
	}
	
	// Create a JSON string from the outputMap
	public String outputJSON () {
		// make sure we've executed first
		if(!executed) execute();
		return outputMap.toJSON();
	}

	// Create a CSV string from the outputMap
	public String outputCSV () {
		// make sure we've executed first
		if(!executed) execute();
		String csv = "";
		return csv;
	}

	// Generate interactive form HTML from the outputMap
	public String outputForm () {
		System.out.println("Query: outputing form HTML...");
		// make sure we've executed first
		if(!executed) execute();
		// also map the data into columns
		columns();
		//System.out.println( "Query: outputMap: "+outputMap );
		// start HTML and start the form table
		String html = 
			"<html>\n<head>\n<title>CanoeDB</title>\n<style>\n"+
			"body { font-family:sans-serif; }"+
			"div { width:100%; overflow-x:auto; overflow-y:hidden; }\n"+
			"table { border-collapse:collapse; table-layout:fixed; }\n"+
			"th, td { padding:10px; text-align:left; }\n"+
			//"table td {border:solid 1px #eee; word-wrap:break-word;}\n"+
			//"table th {border:solid 1px #eee; word-wrap:break-word;}\n"+
			"</style></head>\n<body>\n<div>\n<form id=\"main_form\">\n<table>\n<tr>\n";
		// loop through all the tables and columns
		for (String table : db.tables()) {
			System.out.println("Query: table columns: "+table+" "+db.table(table).columns());
			for (String column : db.table(table).columns()) {
				html +=
					"<td>"+table+"<br>"+column+"<br>"+
					"<input name=\""+table+"."+column+"\" list=\""+table+"."+column+"_list\" "+
					"value=\""+(inputTemplate.read(table, column)!=null ? inputTemplate.read(table, column) : "")+"\" "+
					"onchange=\"document.getElementById('main_form').submit()\" "+
					"size=5>\n"+
					"</td>\n<datalist id=\""+table+"."+column+"_list\">\n";
				for (String data : colMap.keys(table, column)) {
					html += "<option value=\""+data+"\">\n";
				}
				html += "</datalist>\n";
			}
		}
		// complete the form table and start the output table
		html += "</tr>\n</table>\n</form>\n</div>\n<br>\n<div>\n<table>\n<tr>";
		// table headers
		for (String row : outputMap.keys()) {
			for (String table : outputMap.keys(row)) {
				for (String column : outputMap.keys(row, table)) {
					html += "<th>"+table+"<br>"+column+"</th>\n";
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
		html += "</table>\n</div>\n</body>\n</html>\n";
		return html;
	}
	
	// Set the output mode for this query
	public Query command (String c) {
		System.out.println("Query: command \""+c+"\"");
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
				// if we're switching to write and have already executed, then re-execute
				if(!write && executed) {
					write = true;
					execute();
				} else write = true;
				break;
			case "read" :
				write = false;
				break;
			case "columns" :
				// load the colMap
				columns();
				outputMap = colMap;
				break;
			case "rows" :
				outputMap = rowMap;
				break;
			case "json" :
				output = outputJSON();
				mime = "application/json; charset=utf-8";
				break;
			case "csv" :
				output = outputCSV();
				mime = "text/csv; charset=utf-8";
				break;
			case "form" :
				output = outputForm();
				mime = "text/html; charset=utf-8";
				break;
		}
		return this;
	}

	// Get the output String from this query
	public String output () {
		if(!executed) {
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
