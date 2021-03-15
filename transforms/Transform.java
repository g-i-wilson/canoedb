package canoedb.transforms;

import java.util.*;
import canoedb.*;

public class Transform {

	public Transform () {
		System.out.println( this+": initialized!" );
	}

	// transform a data String on reading from a TableRow
	public String onRead ( String s ) {
		return s;
	}
	
	// transform a data String on writing to a TableRow
	public String onWrite ( String s ) {
		return s;
	}
	
	// get a Collection of TableRow objects based on searchString (post onRead transform)
	public Collection<TableRow> tableRows (Table t, String column, String searchString) {
		// utilizes the search() function of the tableIndex that finds TableRows based on whole data element or word-begins-with fragment
		return t.tableIndex.search( column, searchString );
	}

	
}