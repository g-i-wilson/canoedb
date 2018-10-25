package io.github.gabrielwilson3.canoedb;

import java.util.*;

class StringTreeMap2D<T> {

	Map<String, Map<String, T>> map = new TreeMap<>();

	Map<String, Map<String, T>> map () {
		return this.map;
	}
	
	void write (String a, String b, T t) {
		if (! map.containsKey(a)) map.put(a, new TreeMap<String, T>());
		map.get(a).put(b, t);
	}
	
	void write (String a, Map<String, T> m) {
		map.put(a, m);
	}
	
	// element exists, but it might be null (all we know is the key is there)
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
	T read (String a, String b) {
		if( map.containsKey(a) && map.get(a).containsKey(b) )
			return map.get(a).get(b);
		return null;
	}
	
	Set<String> keys () {
		return map.keySet();
	}
	
	Set<String> keys(String a) {
		if (! map.containsKey(a)) map.put(a, new TreeMap<String, T>());
		return map.get(a).keySet();
	}
	
	StringTreeMap2D cloned () {
		StringTreeMap2D cloned = new StringTreeMap2D();
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