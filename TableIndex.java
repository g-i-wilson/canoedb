package canoedb;

import java.io.*;
import java.nio.file.*;
import java.util.*;
import java.util.regex.Pattern;

public class TableIndex {

	// Table objects
	Table table;
	
	// column -> datafragment -> rowData -> TableRowObject (ignores duplicate rows)
	StringMap3D<TableRow> index = new StringMap3D<>();
		
	// null Collection
	Collection<TableRow> null_collection = new ArrayList<>();

	TableIndex (Table t) {
		table = t;
	}


	TableIndex write (TableRow tr) {
		for (String column : tr.columns()) {
			// strings
			String columnData = tr.read(column);
			String rowHash = tr.hash();
			// index the entire data string
			indexData( column, columnData, rowHash, tr );
			// index each "word" in the data string
			for (String word : columnData.split("\\W+")) {
				if (!word.equals("")) indexData( column, word, rowHash, tr );
			}
		}
		
		// allow chaining
		return this;
	}
	
	public Collection<TableRow> search (String col, String dataFragment) {
		if (dataFragment.equals("")) {
			// return all TableRows
			return table.rowDataMap.values();
		} else if (index.exists(col, dataFragment)) {
			// return filtered TableRows (by column data)
			return index.read(col, dataFragment).values();
		} else {
			// otherwise just return null
			//System.out.println( "TableIndex: no TableRow objects found for: "+col+", "+dataFragment );
			return null_collection;
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
