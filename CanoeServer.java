/*

Based on Webserver.java -- Matt Mahoney, mmahoney@cs.fit.edu


CanoeServer.java provides an HTTP socket to CanoeDB

To run: java CanoeServer /some_directory_with_CSV_files 80 >logfile
To end: <Ctrl-C>

Accepts HTTP GET requests with CGI form-data queries:

  GET /request_text

*/


import java.util.*;
import java.io.*;
import java.net.*;

import io.github.gabrielwilson3.canoedb.*;


public class CanoeServer {
	
	private static ServerSocket serverSocket;
	private static Database database;
	private static int port;

	public static void main(String[] args) throws IOException {

	database 	= ( args.length > 0 ? new Database( args[0] ) : new Database( System.getProperty("user.dir") ) );
	port 		= ( args.length > 1 ? Integer.parseInt(args[1]) : 80 );
	  
	serverSocket=new ServerSocket(port);  // Start, listen on port 80

	while (true) {
			try {
				Socket s = serverSocket.accept();  // Wait for a client to connect
				new ClientHandler(s, database);  // Handle the client in a separate thread
			}
				catch (Exception x) {
				System.out.println(x);
			}
		}
	}
}

// A ClientHandler reads an HTTP request and responds
class ClientHandler extends Thread {
	private Socket socket;  // The accepted socket from the Webserver
	private Database database;

	// Start the thread in the constructor
	public ClientHandler(Socket s, Database d) {
		socket = s;
		database = d;
		start();
	}

	// Read the HTTP request, respond, and close the connection
	public void run() {
		try {
			// Open connections to the socket
			BufferedReader 	in 	= new BufferedReader(new InputStreamReader(socket.getInputStream()));
			PrintStream 	out	= new PrintStream(new BufferedOutputStream(socket.getOutputStream()));

			// Create new Request object
			Request r = new Request( in );
			
			// Create new Query object
			Query q = new Query( database );
			String output = "";
			String mime = "text/html";
			
			// Execute the query
			for (String rest_keyword : r.rest() ) {
				switch (rest_keyword) {
					case "cgi_in" :
						q.inputCGI( r.data() );
						break;
					case "json_in" :
						q.inputJSON( r.data() );
						break;
					case "write" :
						q.write();
						break;
					case "read" :
						q.read();
						break;
					case "columns" :
						q.columns();
						break;
					case "rows" :
						q.rows();
						break;
					case "json_out" :
						output += q.outputJSON();
						mime = "application/json";
						break;
					case "csv_out" :
						output += q.outputCSV();
						mime = "text/csv";
						break;
					case "" :
						// send HTML document here
						mime = "text/html";
						break;
				}
			}
			
			
			//System.out.println( output );


			out.print("HTTP/1.0 200 OK\r\n"+
			  "Content-type: "+mime+"\r\n\r\n");
			out.print( output );
			out.close();
		}
		catch (Exception x) {
			System.out.println("Thread exception caught.");
			x.printStackTrace(System.out);
		}
	}
}

