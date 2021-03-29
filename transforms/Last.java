package canoedb.transforms;

import java.util.*;
import canoedb.*;

public class Last extends Transform {

	@Override
	public Collection<TableRow> tableRows (Table t, String column, String searchString) {
		// utilizes the search() function of the tableIndex that finds TableRows based on whole data element or word-begins-with fragment
		Object[] results = t
			.tableIndex
			.search( column, searchString )
			.toArray();
		Collection<TableRow> last = new ArrayList<>();
		if (results.length>0) {
			TableRow tr = (TableRow) results[results.length-1];
			if (tr!=null) last.add( tr );
		}
		return last;
	}

}
