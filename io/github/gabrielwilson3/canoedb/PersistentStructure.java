package io.github.gabrielwilson3.canoedb;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.io.File;
import java.util.*;


public class PersistentStructure {
	
	// map of Table objects
	HashMap<String, Table> 		tableMap = new HashMap<>();
	StringHashMap2D<TableRow>	trMap = new StringHashMap2D<TableRow>();
	
	// import multiple Tables from a folder
	StringHashMap2D<TableRow> folder ( String folderPath ) {
		dbFolder = new File(folderPath);
		dbName = dbFolder.getName();
		if (dbFolder.exists()) {
			for (File f : dbFolder.listFiles()) {
				if (!f.isDirectory()) {
					Table t = new Table();
					t.read( f );
					tableMap.put( t.name, t ); // Tables to keep in this PersistentStructure
					trMap.write( t.name, t.rowIdMap ); // TableRows to export to the VolatileStructure
				}
			}
		} else {
			System.out.println("PersistentStructure: folder path error: can't find "+folderPath);
		}
		System.out.println( "PersistentStructure: table files loaded: "+tableMap );
		return trMap;
	}
	
	// get Table object (if vivinated, add it to the tableMap)
	Table table ( String tableName ) {
		if (tableMap.containsKey(tableName)) {
			return tableMap.get(tableName);
		} else {
			Table t = new Table();
			t.name = tableName;
			tableMap.put( t.name, t );
			return tableMap.get(tableName);
		}
	}
	
	// get Tables
	String[] tables () {
		return trMap.keys();
	}
	
	// get a TableRow object (if vivinated, add it to the trMap)
	TableRow row ( String tableName, String rowId ) {
		if (trMap.defined(tableName, rowId)) {
			return trMap.read(tableName, rowId);
		} else {
			TableRow tr = table(tableName).row(rowId);
			trMap.write(tableName, rowId, tr);
			return tr;
		}
	}
	
	// get TableRows
	String[] rows (String table) {
		return trMap.keys( table );
	}
	
}