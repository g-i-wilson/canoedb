package io.github.gabrielwilson3.canoedb;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.io.File;
import java.util.*;


public class Database {
	
	// Database root path (default location for any new Tables)
	File databaseFolder;
	
	// map of Table objects
	StringMap1D<Table> tableMap = new StringMap1D<>();
	
	// constructor
	public Database ( String f ) {
		folder( f );
		link();
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
				tableMap.write( t.name, t ); // Tables to keep in this PersistentStructure
				System.out.println("Database: data in "+t.name+": "+t.tableIndex.index.toString());
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
		} else
			System.out.println("Database: ERROR can't find directory "+d);
		System.out.println("Database: all tables loaded:\n"+tableMap);
		return this;
	}
	
	// link all the TableRow objects together by direct links
	public Database link () {
		for (String table : tables()) {
			for (String rowId : rows(table)) {
				TableRow tr = row(table, rowId);
				StringMap1D<String> refMap = tr.table.referenceMap;
				for (String refCol : refMap.keys()) {
					// if there's a referenced table in the column
					if (refMap.defined(refCol) && !refMap.read(refCol).equals("")) {
						String refTable = refMap.read(refCol);
						String refRow = tr.data(refCol);
						System.out.println( "Database: reference found "+refTable+", "+refRow );
						if ( !refRow.equals("") ) {
							TableRow linked_tr = row(refTable, refRow);
							// link to
							if (!tr.to.contains(linked_tr)) tr.to( linked_tr );
							// link from
							if (!linked_tr.from.contains(tr)) linked_tr.from( tr );
						}
					}
				}
			}
		}
		System.out.println("Database: table row-linking complete.");
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
		return tableMap.keys();
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
		for (String t : q.inputTemplate.keys()) {
			for (String c : q.inputTemplate.keys( t )) {
				// get the first data fragment from inputTemplate
				String data = q.inputTemplate.read( t, c );
				Collection<TableRow> results = table( t ).search( c, data );
				// if no search results from its table, then if write-mode, create a new TableRow
				if (q.write && results.size()==0) {
					TableRow tr = table( t ).row();
					for (String col : q.inputTemplate.keys( t ))
						tr.update( col, q.inputTemplate.read( t, c ) );
					// ************ create new linked TableRows here *************
				}
				// loop through the TableRows in the search results and populate the Query object
				for (TableRow tr : results) {
					System.out.println( "Database: querying row: "+tr );
					tr.read( q );
				}
			}
		}
	}
	
	// create a new Query
	public Query query () {
		return new Query( this );
	}

	
}
