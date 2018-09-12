package io.github.gabrielwilson3.canoedb;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.io.File;
import java.util.*;


public class Database {
	
	Map<String, Table> tableMap = new HashMap<>();
	
	String dbName;
	File dbFolder;

	// Construct a database from a folder path
	public Database(String folderPath) {
		dbFolder = new File(folderPath);
		dbName = dbFolder.getName();
		if (dbFolder.exists()) {
			for (File f : dbFolder.listFiles()) {
				if (!f.isDirectory()) {
					Table t = new Table( this, new TableFile( f ) );
					tableMap.put( t.name(), t );
				}
			}
		} else {
			System.out.println("Database: folder path error: can't find "+folderPath);
		}
		System.out.println( "Database: table files loaded: "+tableMap );
	}

	// Access a table by its name
	public Table tables (String tableName) {
		if (tableMap.containsKey(tableName)) {
			return tableMap.get(tableName);
		} else {
			return null;
		}
	}
	
	// Create a new table with a TableFile object
	public Table add (TableFile tf) {
		Table t = new Table( this, tf );
		tableMap.put( t.name(), t );
		return t;
	}
	
	// Access database name
	public String name () {
		return this.dbName;
	}

}
