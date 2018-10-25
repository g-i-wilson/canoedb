package io.github.gabrielwilson3.canoedb;

import java.util.*;

class StringMap2D<T> {

	Map<String, Map<String, T>> map = new LinkedHashMap<>();
	Set<String> null_set = new LinkedHashSet<>();

	Map<String, Map<String, T>> map () {
		return map;
	}
	
	void write (String a, String b, T t) {
		if (! map.containsKey(a)) map.put(a, new LinkedHashMap<String, T>());
		map.get(a).put(b, t);
	}
	
	void write (String a, Map<String, T> m) {
		map.put(a, m);
	}
	
	// element exists, but it might be null (all we know is the key is there)
	boolean exists (String a) {
		if( map.containsKey(a) )
			return true;
		return false;
	}
	boolean exists (String a, String b) {
		if( map.containsKey(a) && map.get(a).containsKey(b) )
			return true;
		return false;
	}
	
	// element exists + contains something (it's not null)
	boolean defined (String a, String b) {
		if( map.containsKey(a) && map.get(a).containsKey(b) && map.get(a).get(b)!=null )
			return true;
		return false;
	}
	
	// returns the reference to the object (or null)
	Map<String, T> read (String a) {
		if( map.containsKey(a) )
			return map.get(a);
		return null;
	}
	T read (String a, String b) {
		if( map.containsKey(a) && map.get(a).containsKey(b) )
			return map.get(a).get(b);
		return null;
	}
	
	Set<String> keys () {
		return map.keySet();
	}
	
	Set<String> keys(String a) {
		if (! map.containsKey(a)) return null_set;
		return map.get(a).keySet();
	}
	
	StringMap2D cloned () {
		StringMap2D cloned = new StringMap2D();
		for ( String a : this.keys() ) {
			for ( String b : this.keys(a) ) {
				cloned.write( a, b, this.read(a,b) );
			}
		}
		return cloned;
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
		return map.toString(); 
	}
	
}