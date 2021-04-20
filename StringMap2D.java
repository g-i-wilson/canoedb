package canoedb;

import java.util.*;

public class StringMap2D<T> {

	private Map<String, Map<String, T>> map = new LinkedHashMap<>();
	private Set<String> null_set = new LinkedHashSet<>();
	private String hashStr = "";
	private boolean mapChanged = false;

	// access or change internal map object
	public Map<String, Map<String, T>> map () {
		return map;
	}
	public StringMap2D map ( Map<String, Map<String, T>> m ) {
		map = m;
		mapChanged = true;
		return this;
	}

	// write
	public StringMap2D<T> write (String a, String b, T t) {
		if (! map.containsKey(a)) map.put(a, new LinkedHashMap<String, T>());
		map.get(a).put(b, t);
		mapChanged = true;
		return this;
	}
	public StringMap2D<T> write (String a, Map<String, T> m) {
		map.put(a, m);
		mapChanged = true;
		return this;
	}

	// vivify
	public StringMap2D<T> vivify (String a) {
		if (! map.containsKey(a)) map.put(a, new LinkedHashMap<String, T>());
		mapChanged = true;
		return this;
	}
	public StringMap2D<T> vivify (String a, String b) {
		if (! map.containsKey(a)) map.put(a, new LinkedHashMap<String, T>());
		map.get(a).put(b, null);
		mapChanged = true;
		return this;
	}

	// element exists, but it might be null (all we know is the key is there)
	public boolean exists (String a) {
		if( map.containsKey(a) )
			return true;
		return false;
	}
	public boolean exists (String a, String b) {
		if( map.containsKey(a) && map.get(a).containsKey(b) )
			return true;
		return false;
	}

	// element exists + contains something (it's not null)
	public boolean defined (String a, String b) {
		if( map.containsKey(a) && map.get(a).containsKey(b) && map.get(a).get(b)!=null )
			return true;
		return false;
	}

	// returns the reference to the object (or null)
	public Map<String, T> read (String a) {
		if( map.containsKey(a) )
			return map.get(a);
		return null;
	}
	public T read (String a, String b) {
		if( map.containsKey(a) && map.get(a).containsKey(b) )
			return map.get(a).get(b);
		return null;
	}

	public Set<String> keys () {
		return map.keySet();
	}

	public Set<String> keys(String a) {
		if (! map.containsKey(a)) return null_set;
		return map.get(a).keySet();
	}

	public StringMap2D<T> cloned () {
		StringMap2D<T> cloned = new StringMap2D<>();
		for ( String a : this.keys() ) {
			cloned.vivify(a);
			for ( String b : this.keys(a) ) {
				cloned.write( a, b, this.read(a,b) );
			}
		}
		return cloned;
	}
	public StringMap1D<T> cloned (String a) {
		StringMap1D<T> cloned = new StringMap1D<>();
		for ( String b : this.keys(a) ) {
			cloned.write( b, this.read(a,b) );
		}
		return cloned;
	}

	public StringMap1D<T> referenced (String a) {
		StringMap1D<T> ref = new StringMap1D<>();
		ref.map( read(a) );
		return ref;
	}

	boolean allNulls () {
		for ( String a : this.keys() ) {
			for ( String b : this.keys(a) ) {
				if (map.get(a).get(b) != null) return false;
			}
		}
		return true;
	}

	boolean noNulls () {
		for ( String a : this.keys() ) {
			for ( String b : this.keys(a) ) {
				if (map.get(a).get(b) == null) return false;
			}
		}
		return true;
	}

	@Override
	public String toString() {
		return toJSON();
	}

	public String toJSON() {
		String output = "{";
		String a_comma = "\n";
		for ( String a : keys() ) {
			if (a==null) continue;
			output += a_comma+"\t\""+a.toString().replace("\"","\\\"")+"\" : {";
			a_comma = ",\n";
			String b_comma = "\n";
			for ( String b : keys(a) ) {
				if (b==null) continue;
				T data = read(a,b);
				if (data!=null) {
					output += b_comma+"\t\t\""+b.toString().replace("\"","\\\"")+"\" : \""+data.toString().replace("\"","\\\"")+"\"";
				} else {
					output += b_comma+"\t\t\""+b.toString().replace("\"","\\\"")+"\" : null";
				}
				b_comma = ",\n";
			}
			output += "\n\t}";
		}
		return output+"\n}";
	}

	public String hash () { // how this is implemented may change
		if (mapChanged) {
			hashStr = map.toString();
			mapChanged = false;
			return hashStr;
		} else {
			return hashStr;
		}
	}

}
