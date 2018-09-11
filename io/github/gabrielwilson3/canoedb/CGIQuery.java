package io.github.gabrielwilson3.canoedb;

import java.util.*;
import java.net.URLDecoder;
import java.net.URLEncoder;

public class CGIQuery extends Query {
	
	public CGIQuery (Database d, String request) {
		// copy database reference
		this.dbObject = d;
		// loop through cgi data and directly map input, output, and operation data
		String[] post_ques = request.split("\\?"); // extract data after the last '?'
		String[] before_space = post_ques[post_ques.length-1].split(" "); // extract data before the first space
		String[] touples = before_space[0].split("&"); // divide up extracted data using '&'
		for (int i=0;i<touples.length;i++) {
			String[] touple = touples[i].split("=");
			try {
				String[] table_column = decodeURI(touple[0]).split("\\.");
				String data = ( touple.length>1 ? decodeURI(touple[1]) : "" );
				String table = table_column[0];
				String column = table_column[1];
				if (data.equals("")) {
					// empty data string makes this an output
					this.output(table, column);
					//System.out.println("OUTPUT: table="+table+", column="+column);
				} else {
					// something in data makes this an input
					this.input(table, column, data);
					//System.out.println("INPUT: table="+table+", column="+column+", data="+data);
				}
			} catch(Exception e) {
				System.out.println("Didn't understand touple: "+touples[i]);
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
