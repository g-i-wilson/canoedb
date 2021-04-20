package canoedb;

import java.util.*;

public class StringMap1D<T> {

	private Map<String, T> map = new LinkedHashMap<>();
	private Set<String> null_set = new LinkedHashSet<>();
	private String hashStr = "";
	private boolean mapChanged = false;

	// access or change internal map object
	public Map<String, T> map () {
		return map;
	}
	public StringMap1D<T> map ( Map<String, T> m ) {
		map = m;
		mapChanged = true;
		return this;
	}

	// write
	public StringMap1D<T> write (String a, T t) {
		map.put(a, t);
		mapChanged = true;
		return this;
	}

	// element exists, but it might be null (all we know is the key is there)
	public boolean exists (String a) {
		return map.containsKey(a);
	}

	// element exists + contains something (it's not null)
	public boolean defined (String a) {
		return ( map.containsKey(a) && map.get(a)!=null );
	}

	// returns a reference to the object
	public T read (String a) {
		if( map.containsKey(a) )
			return map.get(a);
		return null;
	}

	public Set<String> keys () {
		return map.keySet();
	}

	public Collection<T> values () {
		return map.values();
	}

	public StringMap1D<T> cloned () {
		StringMap1D<T> cloned = new StringMap1D<>();
		for ( String a : this.keys() ) {
			cloned.write( a, this.read(a) );
		}
		return cloned;
	}

	public boolean allNulls () {
		for ( String a : this.keys() ) {
			if (map.get(a) != null) return false;
		}
		return true;
	}

	public boolean noNulls () {
		for ( String a : this.keys() ) {
			if (map.get(a) == null) return false;
		}
		return true;
	}

	// loop through THIS and overwrite THIS with M (non-null)
	public StringMap1D<T> update ( StringMap1D<T> m ) {
		for ( String a : this.keys() )
			if (m.defined(a)) write(a, m.read(a));
		return this;
	}

	// loop through M and overwrite THIS with M (non-null)
	public StringMap1D<T> merge ( StringMap1D<T> m ) {
		for ( String a : m.keys() )
			if (m.defined(a)) write(a, m.read(a));
		return this;
	}

	// export a template object of this map (pre-initialize values)
	public StringMap1D<T> templated (T b) {
		StringMap1D<T> m = new StringMap1D<>();
		for ( String a : keys() )
			m.write(a, b);
		return m;
	}

	// join map values into a delimited string
	public String join ( String delim ) {
		String delim_str = "";
		String output_str = "";
		for ( String a : keys() ) {
			output_str += delim_str+read(a);
			delim_str = delim;
		}
		return output_str;
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
			T data = read(a);
			if (data!=null) {
				output += a_comma+"\t\""+a.toString().replace("\"","\\\"")+"\" : \""+data.toString().replace("\"","\\\"")+"\"";
			} else {
				output += a_comma+"\t\""+a.toString().replace("\"","\\\"")+"\" : null";
			}
			a_comma = ",\n";
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
