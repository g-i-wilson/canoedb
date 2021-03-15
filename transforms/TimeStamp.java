package canoedb.transforms;

import java.util.*;
import java.time.LocalDateTime;
import canoedb.*;

public class TimeStamp extends Transform {
	
	// auto
	@Override
	public String onWrite ( String s ) {
		if (s.toLowerCase().equals("auto")) {
			return LocalDateTime.now().toString();
		} else {
			return s;
		}
	}
	@Override
	public Collection<TableRow> tableRows (Table t, String column, String searchString) {
		if (searchString.toLowerCase().equals("auto")) {
			return null; // make this filter N/A
		} else {
			return t.tableIndex.search( column, searchString );
		}
	}


}