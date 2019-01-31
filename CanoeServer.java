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
import io.github.gabrielwilson3.canoedb.transforms.*;


public class CanoeServer {
	
	private static ServerSocket serverSocket;
	private static Database database;
	private static int port;

	public static void main(String[] args) throws IOException {

	database 	= ( args.length > 0 ? new Database( args[0] ) : new Database( System.getProperty("user.dir") ) );
	port 		= ( args.length > 1 ? Integer.parseInt(args[1]) : 80 );
	  
	serverSocket=new ServerSocket(port);  // Start, listen on port 80
	
	int sessionId = 0;

	while (true) {
			try {
				Socket socket = serverSocket.accept();  // Wait for a client to connect
				sessionId++;
				new ClientHandler(socket, database, sessionId);  // Handle the client in a separate thread
			}
				catch (Exception e) {
				System.out.println("CanoeServer: Server exception caught.");
				System.out.println(e);
				e.printStackTrace(System.out);
			}
		}
	}
}

// A ClientHandler reads an HTTP request and responds
class ClientHandler extends Thread {
	private Socket socket;  // The accepted socket from the Webserver
	private Database database;
	private int sessionId;

	// Start the thread in the constructor
	public ClientHandler(Socket s, Database d, int n) {
		socket = s;
		database = d;
		sessionId = n;
		start();
	}

	// Read the HTTP request, respond, and close the connection
	public void run() {
		try {
			
			// Log session start
			System.out.println("\n\n====\n["+sessionId+"] ClientHandler: session opened...");
			
			try {
				// Object namespace and defaults
				Request req = new Request( socket, sessionId );
				Response res = new Response( socket, sessionId );
				Query q = new Query( database, sessionId );
				String format = "spa";
				boolean writeMode = false;
				String logic = "and";
				boolean nullsAllowed = false;
				boolean zeroLengthFiltersEnabled = false;
				
				try {
					// Configure the query using the requested path as settings
					for ( String setting : req.path() ) {
						switch (setting.toLowerCase()) {
							// document type
							case "json" : 		format = 		"json";	break;
							case "csv" : 		format = 		"csv"; 	break;
							case "form" : 		format = 		"form";	break;
							
							// read-only (read) or write-read (write)
							case "write" : 		writeMode =		true; 	break;
							case "read" : 		writeMode =		false; 	break;
							
							// logic
							case "and" : 		logic = 		"and"; 	break;
							case "or" : 		logic = 		"or"; 	break;
							case "xor" : 		logic = 		"xor"; 	break;
							
							// are nulls allowed?
							case "nulls" :		nullsAllowed = 	true; 	break;
							case "nonulls" :	nullsAllowed = 	false; 	break;
							
							// are zero length ("") filters enabled (turns all "outputs" into "filters")?
							case "zero" :		zeroLengthFiltersEnabled = 	true; 	break;
							case "nonzero" :	zeroLengthFiltersEnabled = 	false; 	break;
						}
					}
					
					// Log query settings
					System.out.println("["+sessionId+"] ClientHandler:"+
						" format:"+format+
						", writeMode:"+writeMode+
						", logic:"+logic+
						", nulls:"+nullsAllowed+
						", zero-length-filters:"+zeroLengthFiltersEnabled
					);
				} catch (Exception e) {
					System.out.println("["+sessionId+"] ClientHandler: ERROR unable to read request from socket.");
					System.out.println(e);
					e.printStackTrace(System.out);
				}
				
				try {
					// Execute the query with any query data received
					q.execute( req.data(), writeMode, logic, nullsAllowed, zeroLengthFiltersEnabled );
					// Print database traversal log
					System.out.print(q.logString());
				} catch (Exception e) {
					System.out.println("["+sessionId+"] ClientHandler: ERROR unable to execute query.");
					System.out.println(e);
					e.printStackTrace(System.out);
				}
				
				try {
					// Write the response
					res.write( q, format );
				} catch (Exception e) {
					System.out.println("["+sessionId+"] ClientHandler: ERROR unable to write response to socket.");
					System.out.println(e);
					e.printStackTrace(System.out);
				}
				
			} catch (Exception e) {
				System.out.println("["+sessionId+"] ClientHandler: ERROR thread exception (socket closed gracefully).");
				System.out.println(e);
				e.printStackTrace(System.out);
			}
			
			// Close the socket
			socket.close();
			
			// Log session end
			System.out.println("["+sessionId+"] ClientHandler: session closed.\n====\n\n");
			
		} catch (Exception e) {
			System.out.println("["+sessionId+"] ClientHandler: ERROR thread exception (ungracefully caught exception).");
			System.out.println(e);
			e.printStackTrace(System.out);
		}
	}
}

