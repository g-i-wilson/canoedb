package io.github.gabrielwilson3.canoedb;

import java.util.*;
import java.io.*;
import java.net.*;

public class Response {
	
	int sessionId;
	Socket socket;
	
	// init
	public Response ( Socket s, int id ) {
		sessionId = id;
		socket = s;
	}
	
	// output to the socket
	void output ( String body, String mime ) {
		try {
			PrintWriter out	= new PrintWriter(socket.getOutputStream(), true); // autoFlush true
			// Print the HTTP text string
			out.print(
				"HTTP/1.0 200 OK\r\n"+
				"Content-type: "+mime+"; charset=utf-8\r\n"+
				"Access-Control-Allow-Origin: *\r\n"+
				"\r\n"+
				body
			);
			// close the connection
			out.close();
		} catch (Exception e) {
			
		}
	}
	
	// SPA
	public void outputResource ( String r ) {
		output( "<h1>"+r+"</h1>", "text/html" );
	}
	
	// JSON
	public void outputJSON ( Query q ) {
		System.out.println(sessionId+" -> Responce: generating JSON...");
		String body =
			"{\n"+
			"\"name\" : \""+
			q.db.name()+
			"\",\n"+
			"\"structure\" : "+
			q.structMap.toJSON()+
			",\n"+
			"\"columns\" : "+
			q.colMap.toJSON()+
			",\n"+
			"\"rows\" : "+
			q.rowMap.toJSON()+
			"\n}";
			
		output( body, "application/json" );
	}

	// CSV
	public void outputCSV ( Query q ) {
		System.out.println(sessionId+" -> Responce: generating CSV...");
		String csv = "";
		
		output( csv, "text/csv" );
	}
	
	// HTML Form
	public void outputForm ( Query q ) {
		System.out.println(sessionId+" -> Responce: generating form HTML...");
		// start HTML and start the form table
		String html = 
			"<html>\n<head>\n<title>CanoeDB</title>\n<style>\n"+
			"body { font-family:sans-serif; }"+
			"div { width:100%; overflow-x:auto; overflow-y:hidden; }\n"+
			"table { border-collapse:collapse; table-layout:fixed; }\n"+
			"th, td { padding:10px; text-align:left; }\n"+
			"</style></head>\n<body>\n<div>\n<form id=\"main_form\" method=\"post\">\n<table>\n<tr>\n";
		// loop through all the tables and columns
		for (String table : q.db.tables()) {
			Table t = q.db.table(table);
			for (String column : t.columns()) {
				if (t.reference(column).equals("")) {
					html +=
						"<td>"+table+"<br>"+column+"<br>"+
						"<input name=\""+table+"."+column+"\" list=\""+table+"."+column+"_list\" "+
						"value=\""+(q.inputTemplate.read(table, column)!=null ? q.inputTemplate.read(table, column) : "")+"\" "+
						"onchange=\"document.getElementById('main_form').submit()\" "+
						"onblur=\"document.getElementById('main_form').submit()\" "+
						"onfocus=\"this.value=''\" "+
						"size=5>\n"+
						"</td>\n<datalist id=\""+table+"."+column+"_list\">\n";
					for (String data : q.colMap.keys(table, column)) {
						html += "<option value=\""+data+"\">\n";
					}
					html += "</datalist>\n";
				}
			}
		}
		// complete the form table and start the output table
		html += "</tr>\n</table>\n</form>\n</div>\n<br>\n<div>\n<table>\n<tr>";
		// table headers
		for (String row : q.rowMap.keys()) {
			for (String table : q.rowMap.keys(row)) {
				Table t = q.db.table(table);
				for (String column : q.rowMap.keys(row, table)) {
					if (t.reference(column).equals("")) {
						html += "<th>"+table+"<br>"+column+"</th>\n";
					}
				}
			}
			break;
		}
		html += "</tr>\n";
		// table rows
		for (String row : q.rowMap.keys()) {
			html += "<tr>\n";
			for (String table : q.rowMap.keys(row)) {
				Table t = q.db.table(table);
				for (String column : q.rowMap.keys(row, table)) {
					if (t.reference(column).equals("")) {
						String dataElement = q.rowMap.read(row, table, column);
						html += "<td>"+dataElement+"</td>\n";
						//html += "<td>"+( dataElement!=null ? dataElement : "" )+"</td>\n";
					}
				}
			}
			html += "</tr>\n";
		}
		// complete the output table and complete HTML
		html += "</table>\n</div>\n</body>\n</html>\n";
		System.out.println(sessionId+" -> Responce: HTML complete");
		
		output( html, "text/html" );
	}
	
}
