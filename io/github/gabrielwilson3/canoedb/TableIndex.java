package io.github.gabrielwilson3.canoedb;

import java.io.*;
import java.nio.file.*;
import java.util.*;
import java.util.regex.Pattern;

class TableIndex {

	// Table columns (for finding row by column data)
	// column -> datafragment -> rowData -> TableRowObject (ignores duplicate rows)
	StringMap3D<TableRow> index = new StringMap3D<>();


	TableIndex write (TableRow tr) {
		for (String column : tr.data.keys()) {
			String columnData = tr.data(column);
			// index the entire data string
			indexData( column, columnData, tr.data.map.toString(), tr );
			// index each "word" in the data string
			for (String word : columnData.split("\\W+")) {
				if (!word.equals("")) indexData( column, word, tr.data.map.toString(), tr );
			}
		
		}
		
		// allow chaining
		return this;
	}
	
	Collection<TableRow> search (String col, String dataFragment) {
		if (index.exists(col, dataFragment)) {
			return index.read(col, dataFragment).values();
		} else {
			// otherwise just return null
			System.out.println( "TableIndex: no TableRow objects found for: "+col+", "+dataFragment );
			return null;
		}
	}
	
	private void indexData( String col, String data, String hash, TableRow tr ) {
		// index each begins-with slice of the data string
		for (int i=0; i<data.length(); i++) {
			// slice
			String begins_with = data.substring( 0, i+1 );
			index.write( col, begins_with, hash, tr );
		}
	}
	
}
