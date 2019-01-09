package io.github.gabrielwilson3.canoedb;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.io.File;
import java.util.*;
import io.github.gabrielwilson3.canoedb.transforms.*;


public class Database {
	
	// Database root path (default location for any new Tables)
	File databaseFolder;
	
	// map of Table objects
	StringMap1D<Table> tableMap = new StringMap1D<>();
	
	// load Transform classes on demand
	ClassLoader classLoader = Database.class.getClassLoader();
	Transform null_transform = new Transform();
	StringMap1D<Transform> transformMap = new StringMap1D<>();

	// constructor
	public Database ( String f ) {
		folder( f );
		linkTables();
		linkTableRows();
	}
	
	// import a Table from a String file path
	public Database file ( String f ) {
		file( new File( f ) );
		return this;
	}

	// import a Table from a File object
	public Database file ( File f ) {
		if (f.exists()) {
			Table t = new Table( f );
			if (t.read()) {
				System.out.println("Database: loaded table "+t.name);
				tableMap.write( t.name, t );
			} else System.out.println("Database: ERROR couldn't load table "+t.name);
		} else System.out.println("Database: ERROR can't find file "+f);
		return this;
	}

	// import multiple Tables from a folder
	public Database folder ( String f ) {
		folder( new File ( f ) );
		return this;
	}
	
	// import multiple Tables from a folder
	public Database folder ( File d ) {
		if (d.exists()) {
			databaseFolder = d;
			for (File f : d.listFiles())
				if (f.isDirectory() && !f.getName().equals("..") && !f.getName().equals(".")) {
					System.out.println("Database: reading "+f);
					folder( f );
				} else
					file( f );
		} else {
			System.out.println("Database: ERROR can't find directory "+d);
		}
		return this;
	}
	
	// link all the Table objects together by direct links
	public Database linkTables () {
		System.out.println("\nDatabase: Linking Tables...");
		for (String tableName : tables()) {
			StringMap1D<String> refMap = table(tableName).referenceNames;
			Table t = table(tableName);
			// LINK Tables
			for (String refCol : refMap.keys()) {
				if (refMap.defined(refCol) && !refMap.read(refCol).equals("")) {
					String refTable = refMap.read(refCol);
					Table linked_t = table(refTable);
					// link to Table
					t.linkTo( refCol, linked_t );
					// link from Table
					linked_t.linkFrom( t );
				}
			}
		}
		// now that we've linked all Tables and TableRows, fill the toMapAll in each table
		for (String tableName : tables()) {
			table(tableName).downhill();
		}
		System.out.println("Database: Table linking complete.");
		return this;
	}
	
	// link all the TableRow objects together by direct links
	public Database linkTableRows () {
		System.out.println("\nDatabase: Linking TableRows...");
		for (String tableName : tables()) {
			StringMap1D<String> refMap = table(tableName).referenceNames;
			Table t = table(tableName);
			// LINK TableRows
			for (String rowId : rows(tableName)) {
				TableRow tr = row(tableName, rowId);
				for (String refCol : refMap.keys()) {
					// if there's a referenced table in the column
					if (refMap.defined(refCol) && !refMap.read(refCol).equals("")) {
						String refTable = refMap.read(refCol);
						String refRow = tr.read(refCol);
						if ( !refRow.equals("") ) {
							TableRow linked_tr = row(refTable, refRow);
							// link to TableRow
							tr.linkTo( linked_tr );
							// link from TableRow
							linked_tr.linkFrom( tr );
						}
					}
				}
			}
		}
		System.out.println("Database: TableRow linking complete.");
		return this;
	}

	// get a Table object (auto-vivifies)
	public Table table ( String tableName ) {
		if (tableMap.defined(tableName)) {
			return tableMap.read(tableName);
		} else {
			File tableFile = new File( databaseFolder, tableName+".csv" );
			Table t = new Table(tableFile); // won't create an actual file until the first TableRow is appended
			tableMap.write( t.name, t );
			System.out.println( "Database: auto-vivified table "+t.name);
			return t;
		}
	}
	
	// get all Tables
	public Set<String> tables () {
		return tableMap.cloned().keys();
	}
	
	// get a TableRow object (auto-vivifies)
	public TableRow row ( String table, String rowId ) {
		return table( table ).row( rowId );
	}
	
	// get all TableRows
	public Set<String> rows (String table) {
		return table( table ).rows();
	}
	
	// run a read or write-read query using a Query object
	void execute ( Query q ) {
		q.log("Database: executing query...");
		for (String tableName : q.inputTemplate.keys()) {
			// get a reference to the Table
			Table t = table( tableName );
			// if this Table file doesn't exist yet, then use the inputTemplate map to configure the columns
			if (!t.fileExists) {
				q.log("Database: table file doesn't exist...");
				t.initToDisk( q );
				linkTables();
			}
			// WRITE (writes the string as-is)
			if (q.write) {
				q.log("Database: starting WRITE...");
				q.log( "Database: WRITE traverse starting at Table "+tableName );
				t.write( q );
			}
			// READ (searches for the string as though it's a begins-with fragment)
			q.log("Database: starting READ...");
			for (String column : q.inputTemplate.keys( tableName )) {
				q.log("Database: input column "+column+"...");
				for ( TableRow tr : q.rows( t, column ) ) {
					q.log( "Database: READ traverse starting at TableRow "+tr );
					tr.read( q );
				}
			}
		}
	}
	
	// create a new Query
	public Query query ( int connectionId ) {
		return new Query( this, connectionId );
	}
	
	// dynamically load a Class
	public Transform transform ( String tranName ) {
		if (transformMap.defined(tranName)) {
			return transformMap.read(tranName);
		} else {
			// Load a Transform object
			String binName = "io.github.gabrielwilson3.canoedb.transforms."+tranName;
			try {
				Class aClass = classLoader.loadClass(binName);
				Object anObject = aClass.newInstance();
				Transform transformObject = (Transform) anObject;
				transformMap.write( tranName, transformObject );
				System.out.println("Database: loaded Transform object "+binName);
				return transformObject;
			} catch (Exception e) {
				System.out.println("Database: ERROR: unable to load Transform object "+binName);
				e.printStackTrace();
				transformMap.write( tranName, null_transform );
				return null_transform;
			}
		}
	}

	
}
