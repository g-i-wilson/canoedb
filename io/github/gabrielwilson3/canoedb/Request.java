package io.github.gabrielwilson3.canoedb;

import java.util.*;
import java.io.*;
import java.net.*;


public class Request {
	
	String http_request = "";
	StringTokenizer http_tokens;
	String query_data = "";
	String[] path_array = new String[]{};
	
	public Request ( BufferedReader input ) {
		
		// can currently only read one line
		try {
			http_request = input.readLine();
			System.out.println( "\n\n=========\nRequest: read line: "+http_request );
		} catch (Exception e) {
			System.out.println( "\n\n=========\nRequest: error reading from socket" );
			e.printStackTrace( System.out );
		}
		
		// GET request
		if (http_request.contains("GET /")) {
			System.out.println("Request: GET");
			
			System.out.println("Request: ? found");
			// Divide up reqest string at the '?'
			String[] ques_array = http_request.split("\\?");
			
			// Divide up the part before the first '?' (GET /some/path/) using spaces
			String[] first_array = ques_array[0].split(" ");
			path_array = first_array[first_array.length-1].split("/");
			
			// Divide up the part following the last '?' (this=that&it=there HTTP/1.1) using spaces
			String[] last_array = ques_array[ques_array.length-1].split(" ");
			query_data = last_array[0];
			
		// POST request
		} else if (http_request.contains("POST /")) {
			System.out.println("\nCanoeServer: *** currently unable to process POST requests ***\n"+http_request+"\n");
			
		}
		
	}
	
	public String data () {
		return this.query_data;
	}
	
	public String[] path () {
		return this.path_array;
	}

	public String request () {
		return this.http_request;
	}
		
}