package canoedb;

import java.util.*;

public class Query {
	
	// output template
	// table -> column -> null
	Map<String, Map<String, String>> outputTemplate = new HashMap<> ();
	
	// output
	// row_str -> table -> column -> data
	Map<String, Map<String, Map<String, String>>> outputMap = new HashMap<>();
	
	// filter
	// table -> column -> data
	Map<String, Map<String, String>> filterMap = new HashMap<> ();
	
	// database object
	Database dbObject;
	

	public Query (Database db) {
		dbObject = db;
	}
	
	public Map<String, Map<String, Map<String, String>>> map() {
		return outputMap;
	}
	
	Map<String, Map<String, String>> getNewOutput () {
		Map<String, Map<String, String>> templateClone = new HashMap<>();
		// loop through (pseudo-code) table->Map<column,data>
		for ( String table : outputTemplate.keySet() ) {
			Map<String, String> templateCloneColumn = new HashMap<>();
			// loop through (pseudo-code) column->data
			for ( String column : outputTemplate.get(table).keySet() ) {
				templateCloneColumn.put( column, outputTemplate.get(table).get(column) );
			}
			templateClone.put( table, templateCloneColumn );
		}
		return templateClone;
	}
	
	// Add an output
	public Query output (String table, String column) {
		outputTemplate.put( table, new HashMap<String, String>() );
		outputTemplate.get( table ).put( column, null );
		return this;
	}
	
	// Add a filter
	public Query filter (String table, String column, String data) {
		filterMap.put( table, new HashMap<String, String>() );
		filterMap.get( table ).put( column, data );
		return this;
	}
	
	// loop through filters to get each TableRow
	// ask that TableRow to populate outputTemplate
	public Map<String, Map<String, Map<String, String>>> execute() {
		// loop through tables
		for (String table : filterMap.keySet()) {
			// loop through columns
			for (String column : filterMap.get(table).keySet()) {
				// get the data element
				String data = filterMap.get(table).get(column);
				// look up a table row(s) Map by data and column
				System.out.println( "Query: table: "+table+", column: "+column+", data: "+data );
				Table t = this.dbObject.tables(table);
				if (t != null) {
					Map<String, TableRow> rowMap = t.index(column, data);
					System.out.println( "Query: stem table rows: "+rowMap );
					for (TableRow trObject : rowMap.values()) {
						Map<String, Map<String, String>> output = this.getNewOutput();
						// pass the output Map object reference (to be loaded with the output)
						trObject.getData( output );
						// use the "stringified" version of a output HashMap as its key (eliminates any duplicates)
						this.outputMap.put( "key<"+output.toString()+">", output );
						System.out.println( "Query: row: "+trObject.str() );
					}
				}
			}
		}
		return this.outputMap;
	}
	
}