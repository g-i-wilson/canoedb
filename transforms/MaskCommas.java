package canoedb.transforms;

import java.util.*;
import canoedb.*;

public class MaskCommas extends Transform {

	// transform a data String on reading from a TableRow
	public String onRead ( String s ) {
		// eventually this should become "," -> "\," when reading table files
		//    is updated to ignore commas preceded by a '\'.
		// For now, we'll just replace with '|'
		return s.replace('|',',');
	}
	
	// transform a data String on writing to a TableRow
	public String onWrite ( String s ) {
		// See above comment...
		return s.replace(',','|');
	}

	// set a limitation on indexing slices of data Strings
	public int indexSliceLimit (int stringLength) {
		return 0; // disabled
	}

	// split String for indexing words with String.split(___)
	// indexing of words will be skipped if null
	public String indexWordSplitter () {
		return ",";
	}
	
}