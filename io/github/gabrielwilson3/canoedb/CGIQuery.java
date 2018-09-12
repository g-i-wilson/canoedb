package io.github.gabrielwilson3.canoedb;

import java.util.*;
import java.net.URLDecoder;
import java.net.URLEncoder;

public class CGIQuery extends Query {
	
	public CGIQuery (Database d, String request) {
		// copy database reference
		this.dbObject = d;
		// loop through cgi data and directly map input, output, and operation data
		String[] tuples = request.split("&");
		for (int i=0;i<tuples.length;i++) {
			String[] tuple = tuples[i].split("=");
			try {
				String[] table_column = decodeURI(tuple[0]).split("\\.");
				String data = ( tuple.length>1 ? decodeURI(tuple[1]) : "" );
				String table = table_column[0];
				String column = table_column[1];
				if (data.equals("")) {
					// empty data string makes this an output
					this.output(table, column);
					System.out.println("CGIQuery output: table="+table+", column="+column);
				} else {
					// something in data makes this an input
					this.input(table, column, data);
					System.out.println("CGIQuery input: table="+table+", column="+column+", data="+data);
				}
			} catch(Exception e) {
				System.out.println("Didn't understand tuple: "+tuples[i]);
				//e.printStackTrace(System.out);
			}
		}
		// execute this query
		this.execute();
	}
	
	String decodeURI(String value) {
		return URLDecoder.decode(value);
	}
	
	String encodeURI(String value) {
		return URLEncoder.encode(value);
	}
	
}
