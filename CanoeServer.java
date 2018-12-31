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
	
	int connectionId = 0;

	while (true) {
			try {
				Socket socket = serverSocket.accept();  // Wait for a client to connect
				connectionId++;
				new ClientHandler(socket, database, connectionId);  // Handle the client in a separate thread
			}
				catch (Exception x) {
				System.out.println("CanoeServer: Server exception caught.");
				System.out.println(x);
				x.printStackTrace(System.out);
			}
		}
	}
}

// A ClientHandler reads an HTTP request and responds
class ClientHandler extends Thread {
	private Socket socket;  // The accepted socket from the Webserver
	private Database database;
	private int connectionId;

	// Start the thread in the constructor
	public ClientHandler(Socket s, Database d, int n) {
		socket = s;
		database = d;
		connectionId = n;
		start();
	}

	// Read the HTTP request, respond, and close the connection
	public void run() {
		try {
			System.out.println("\n\n****\nClientHandler: connection "+connectionId+" accepted...");
			
			// Query object
			Query			q = database.query();
			q.log("REQUEST INITIATED");
			
			// Open connections to the socket
			//BufferedReader 	in 	= new BufferedReader(new InputStreamReader(socket.getInputStream()));
			//PrintStream 	out	= new PrintStream(new BufferedOutputStream(socket.getOutputStream()));
			PrintWriter 	out	= new PrintWriter(socket.getOutputStream(), true); // autoFlush true
			//DataOutputStream out = new DataOutputStream(socket.getOutputStream());
			q.log("OUTPUT STREAM OPENED");

			// Request object
			Request 		r = new Request( socket, connectionId );
			q.log("Request object created...");
			
			// Send the Request data to the Query
			q.parse( r.data() );
			q.log("Request data parsed...");
			
			// Send each REST command from the Request to the Query
			for ( String keyword : r.path() ) q.command( keyword );
			q.log("Command keywords parsed...");
			
			// Send the HTTP text string back to the client
			out.print(
				"HTTP/1.0 200 OK\r\n"+
				"Content-type: "+q.mime()+"\r\n"+
				"\r\n"+
				q.output()
			);
			out.close();
			q.log("OUTPUT STREAM CLOSED");
			socket.close();
			q.log("SOCKET CLOSED");
			System.out.print( q.logString() );
			
			System.out.println("\nClientHandler: connection "+connectionId+" complete\n****\n\n");
		}
		catch (Exception x) {
			System.out.println("ClientHandler: thread exception caught.");
			x.printStackTrace(System.out);
		}
	}
}

