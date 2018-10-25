package io.github.gabrielwilson3.canoedb;

import java.util.*;

class StringTreeMap2D<T> {

	Map<String, String> map = new TreeMap<>();

	Map<String, String> map () {
		return this.map;
	}
	
	void write (String a, T t) {
		map.get(a).put(b, t);
	}
	
	// element exists, but it might be null (all we know is the key is there)
	boolean exists (String a) {
		return map.containsKey(a);
	}
	
	// element exists + contains something (it's not null)
	boolean defined (String a) {
		return ( map.containsKey(a) && map.get(a)!=null );
	}
	
	// returns the reference to the object (or null)
	T read (String a) {
		if( map.containsKey(a) )
			return map.get(a);
		return null;
	}
	
	Set<String> keys () {
		return map.keySet();
	}
		
	StringTreeMap1D cloned () {
		StringTreeMap2D cloned = new StringTreeMap2D();
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
	
	@Override
	public String toString() { 
		return map.toString(); 
	}
	
}