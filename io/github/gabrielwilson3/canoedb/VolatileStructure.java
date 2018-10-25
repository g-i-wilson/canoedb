package io.github.gabrielwilson3.canoedb;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.io.File;
import java.util.*;


public class VolatileStructure {
	
	VolatileStructure connect (StringHashMap2D<TableRow> tableRowNetwork) {
		for (String table : tableRowNetwork.keys()) {
			for (String rowId : tableRowNetwork.keys(table)) 
				TableRow tr = tableRowNetwork.read(table, rowId);
				for (String referenceColumn : tr.table.referenceMap.keySet()) {
					String referencedTable = tr.table.referenceMap.get(referenceColumn);
					String referencedRow = tr.columns.get(referenceColumn);
					if ( ! referencedRow.equals("") ) {
						TableRow connected_tr = tableRowNetwork.read(referencedTable, referencedRow);
						connected_tr.connections.add( this );
					}
				}
			}
		}
	}
	
	
	TableRow row (String table, String rowId) {
		TableRow tr = tableRowNetwork.read(table, rowId);
		if (tr==null) tr = tableRowNetwork.write(table, rowId, );
	}

	
}