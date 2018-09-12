package io.github.gabrielwilson3.canoedb;

import java.util.*;
import java.io.*;
import java.net.*;


public class Request {
	
	String http_request = "";
	StringTokenizer http_tokens;
	String query_data = "";
	String[] rest_array;
	
	public Request ( BufferedReader input ) {
		
		// can currently only read one line
		try {
			http_request = input.readLine();
			System.out.println( "Request: read line: "+http_request );
		} catch (Exception e) {
			System.out.println( "Request: error reading from socket" );
			e.printStackTrace( System.out );
		}
		
		/*
		// read in the HTTP request from the input
		String line;

		while( (line = input.readLine()) != null ) {
			http_request += " "+line;
		}
		System.out.println( "Request: http_request = "+http_request );
		*/
		
		// can only do GET requests currently
		this.get( http_request );
		
		/*
		st = new StringTokenizer( this.http_request );
		// parse the request string
		if (st.hasMoreElements()) {
			String 
			if( st.nextToken().equalsIgnoreCase("GET") && st.hasMoreElements() ) {
				String rest_path = st.nextToken();
				System.out.println( "Request: REST path = "+rest_path );
				this.get( rest_path );
			} else if ( st.nextToken().equalsIgnoreCase("POST") && st.hasMoreElements() ) {
				String post_request = st.nextToken();
				System.out.println( "Request: POST request = "+post_request );
				this.post( post_request );
			}
        } else {
          System.out.println( "Request: GET request = "+get_request );
		}
		*/
		
	}
	
	void get ( String get_request ) {
		// Divide up reqest string at the '?'
		String[] ques_array = get_request.split("\\?");
		
		// Divide up the part before the first '?' (GET /some/path/) using spaces
		String[] first_array = ques_array[0].split(" ");
		this.rest_array = first_array[first_array.length-1].split("/");
		
		// Divide up the part following the last '?' (this=that&it=there HTTP/1.1) using spaces
		String[] last_array = ques_array[ques_array.length-1].split(" ");
		this.query_data = last_array[0];
	}
	
	public String data () {
		return this.query_data;
	}
	
	public String[] rest () {
		return this.rest_array;
	}
	
}