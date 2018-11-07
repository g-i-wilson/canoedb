package io.github.gabrielwilson3.canoedb;

import java.util.*;

public class StringMap1D<T> {

	Map<String, T> map = new LinkedHashMap<>();
	Set<String> null_set = new LinkedHashSet<>();

	Map<String, T> map () {
		return map;
	}
	
	T write (String a, T t) {
		map.put(a, t);
		return map.get(a);
	}
	
	// element exists, but it might be null (all we know is the key is there)
	boolean exists (String a) {
		return map.containsKey(a);
	}
	
	// element exists + contains something (it's not null)
	boolean defined (String a) {
		return ( map.containsKey(a) && map.get(a)!=null );
	}
	
	// returns a reference to the object
	T read (String a) {
		if( map.containsKey(a) )
			return map.get(a);
		return null;
	}
	
	Set<String> keys () {
		return map.keySet();
	}
	
	Collection<T> values () {
		return map.values();
	}
		
	StringMap1D<T> cloned () {
		StringMap1D<T> cloned = new StringMap1D<>();
		for ( String a : this.keys() ) {
			cloned.write( a, this.read(a) );
		}
		return cloned;
	}
	
	boolean allNulls () {
		for ( String a : this.keys() ) {
			if (map.get(a) != null) return false;
		}
		return true;
	}
	
	boolean noNulls () {
		for ( String a : this.keys() ) {
			if (map.get(a) == null) return false;
		}
		return true;
	}
	
	// loop through THIS and overwrite THIS with M (non-null)
	StringMap1D<T> update ( StringMap1D<T> m ) {
		for ( String a : this.keys() )
			if (m.defined(a)) write(a, m.read(a));
		return this;
	}
	
	// loop through M and overwrite THIS with M (non-null)
	StringMap1D<T> merge ( StringMap1D<T> m ) {
		for ( String a : m.keys() )
			if (m.defined(a)) write(a, m.read(a));
		return this;
	}
	
	// export a template object of this map (pre-initialize values)
	StringMap1D<T> templated (T b) {
		StringMap1D<T> m = new StringMap1D<>();
		for ( String a : keys() )
			m.write(a, b);
		return m;
	}
	
	// join map values into a delimited string
	String join ( String delim ) {
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
	
	String toJSON() {
		String output = "{";
		String a_comma = "\n";
		for ( String a : keys() ) {
			T data = read(a);
			if (data!=null) {
				output += a_comma+"\t\""+a+"\" : \""+data.toString().replace("\"","\\\"")+"\"";
			} else {
				output += a_comma+"\t\""+a+"\" : null";
			}
			a_comma = ",\n";
		}
		return output+"\n}";
	}
	
}