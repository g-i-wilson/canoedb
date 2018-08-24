package canoedb;

import java.io.*;
import java.nio.file.*;
import java.util.*;
import java.util.regex.Pattern;

// Oversees the actual text file on disk
//
// Methods:
//
// TableFile( file_path )
// .init()
// .create()
// .append()
// .data()
//
// .name()
// .setName()
// .columns()
// .setColumns()
// .references()
// .setReferences()
// .onRead()
// .setOnRead()
// .onWrite()
// .setOnWrite()

public class TableFile {
	
	// File header
	String tableName			= "";
	String filePath				= "";
	File fileObject;
	String[] columns			= new String[]{};
	String[] references 		= new String[]{};
	String[] onRead 			= new String[]{};
	String[] onWrite 			= new String[]{};
	
	// File data
	Map<String, String[]> data	= new HashMap<>();
		
	// Constructor
	TableFile ( File f ) {
		this.fileObject = f;
		this.filePath = f.getAbsolutePath();
		// if the file already exists, then read it
		if ( f.exists() ) {
			this.init();
		}
	}
	
	// Create (header -> disk)
	boolean create () {
		try {
			String headerLines =
				"," + this.tableName +
				"\n," + String.join( ",", EncodeArray(this.columns) ) +
				"\n," + String.join( ",", EncodeArray(this.references) ) +
				"\n," + String.join( ",", EncodeArray(this.onRead) ) +
				"\n," + String.join( ",", EncodeArray(this.onWrite) )
			;
			Files.write(this.fileObject.toPath(), headerLines.getBytes());
			return true;
		} catch (Exception e) {
			System.out.println("TableFile: couldn't create file "+this.filePath);
			e.printStackTrace();
			return false;
		}
	}
	
	// Remove first element from an array
	String[] RemoveFirst (String[] a) {
		if (a.length > 1) {
			return Arrays.copyOfRange(a, 1, a.length);
		} else {
			return new String[]{};
		}
	}
	
	String[] EncodeArray (String[] a) {
		for (int i=0; i<a.length; i++) {
			a[i] = a[i].replace( ",", "%2C" );
		}
		return a;
	}
	
	String[] DecodeArray (String[] a) {
		for (int i=0; i<a.length; i++) {
			a[i] = a[i].replace( "%2C", "," );
		}
		return a;
	}

	String[] ChompArray (String[] a) {
		for (int i=0; i<a.length; i++) {
			a[i] = a[i].replace("\r", "");
		}
		return a;
	}
	
	// Initialize (disk -> memory)
	boolean init () {
		
		// whole file
		String wholeFile;
		String[] lines;
		try {
			wholeFile = new String(Files.readAllBytes(this.fileObject.toPath()));
			lines = ChompArray( wholeFile.split("\n") );
		}catch (Exception e) {
			System.out.println("TableFile: read error: couldn't read "+this.filePath);
			e.printStackTrace();
			return false;
		}

		// header arrays
		try {
			this.tableName 	= RemoveFirst( DecodeArray(lines[0].split(",")) )[0];
			this.columns	= RemoveFirst( DecodeArray(lines[1].split(",")) );
			this.references	= RemoveFirst( DecodeArray(lines[2].split(",")) );
			this.onRead		= RemoveFirst( DecodeArray(lines[3].split(",")) );
			this.onWrite	= RemoveFirst( DecodeArray(lines[4].split(",")) );
		} catch (Exception e) {
			System.out.println("TableFile: read error: couldn't parse header structure of "+this.filePath);
			e.printStackTrace();
			return false;
		}
		
		// data array
		for (int i=5; i<lines.length; i++) {
			try {
				String[] elements 	= DecodeArray( lines[i].split(",") );
				// id is first element in the line
				this.data.put( elements[0], RemoveFirst(elements) );
			} catch (Exception e) {
				System.out.println("TableFile: read error: couldn't parse line "+i+" of "+this.filePath);
			}
		}
		
		return true;
	}
	
	
	// Append (update -> disk, then update -> memory)
	boolean append (String id, String[] data) {
		String newLine = "";
		try {
			// create a new line for the file
			newLine = "\n" + id.toString() + "," + String.join( ",", EncodeArray(data) );
			// append the file
			Files.write(this.fileObject.toPath(), newLine.getBytes(), StandardOpenOption.APPEND);
			// append the data map
			this.data.put( id, data );
			return true;
		} catch (Exception e) {
			System.out.println("TableFile: couldn't append '"+newLine+"' to file "+filePath);
			e.printStackTrace();
			return false;
		}
	}
	
	
	// name
	String name() {
		return this.tableName;
	}
	TableFile setName(String s) {
		this.tableName = s;
		return this;
	}
	// columns
	String[] columns() {
		return this.columns;
	}
	TableFile setColumns(String[] s) {
		this.columns = s;
		return this;
	}
	// references
	String[] references() {
		return this.references;
	}
	TableFile setReferences(String[] s) {
		this.references = s;
		return this;
	}
	// onRead
	String[] onRead() {
		return this.onRead;
	}
	TableFile setOnRead(String[] s) {
		this.onRead = s;
		return this;
	}
	// onWrite
	String[] onWrite() {
		return this.onWrite;
	}
	TableFile setOnWrite(String[] s) {
		this.onWrite = s;
		return this;
	}
	
	// data
	Map<String, String[]> data() {
		return this.data;
	}
	
}