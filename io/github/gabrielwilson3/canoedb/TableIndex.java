package io.github.gabrielwilson3.canoedb;

import java.io.*;
import java.nio.file.*;
import java.util.*;
import java.util.regex.Pattern;

class TableIndex {

	// Table columns (for finding row by column data)
	// column -> data -> hash_str -> TableRowObject
	Map<String, Map<String, Map<String, TableRow>>> index = new HashMap<String, Map<String, Map<String, TableRow>>>();
	
	// Null Map of TableRow objects
	Map<String, TableRow> null_map = new HashMap<>();


	TableIndex appendRow (String col, String data, TableRow rowObject) {
		// Check for existance and create new objects
		if (!this.index.containsKey( col ))
			this.index.put( col, new HashMap<String, Map<String, TableRow>>() );
		
		// index each begins-with slice of the data string
		//for (int i=0; i<data.length(); i++) {
			// slice
			//String begins_with = data.substring( 0, i+1 );
			// "vivinate" if necessary
			//if (!this.index.get(col).containsKey(begins_with))
				//this.index.get( col ).put( begins_with, new HashMap<String, TableRow>() );
			// Insert the object
			//this.index.get( col ).get( begins_with ).put( rowObject.str(), rowObject );
		//}
		
		// Index the entire string
		this.indexData( data, rowObject, this.index.get( col ) );
		// Also index all the "words" in the string
		for (String word : data.split(" ")) {
			if (!word.equals("")) this.indexData( word, rowObject, this.index.get( col ) );
		}
		
		// allow chaining
		return this;
	}
	
	void print() {
		System.out.println( "TableIndex: "+index.toString() );
	}
	
	Map<String, TableRow> filterRows (String col, String data) {
		if (this.index.containsKey(col) && this.index.get(col).containsKey(data)) {
			return this.index.get( col ).get( data );
		} else {
			// otherwise just return the "null set" HashMap
			System.out.println( "TableIndex: no TableRow objects found for: "+col+", "+data );
			return null_map;
		}
	}
	
	private void indexData( String data, TableRow rowObject, Map<String, Map<String, TableRow>> map ) {
		// index each begins-with slice of the data string
		for (int i=0; i<data.length(); i++) {
			// slice
			String begins_with = data.substring( 0, i+1 );
			// "vivinate" if necessary
			if (!map.containsKey(begins_with)) map.put( begins_with, new HashMap<String, TableRow>() );
			// Insert the object
			map.get( begins_with ).put( rowObject.str(), rowObject );
		}
	}
	
}
