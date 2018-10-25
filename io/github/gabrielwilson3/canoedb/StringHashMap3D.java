package io.github.gabrielwilson3.canoedb;

import java.util.*;

class StringTreeMap3D<T> {

	Map<String, Map<String, Map<String, T>>> map = new TreeMap<>();

	Map<String, Map<String, Map<String, T>>> map () {
		return this.map;
	}
	
	void write (String a, String b, String c, T t) {
		if (! map.containsKey(a)) map.put(a, new TreeMap<String, TreeMap<String, T>>());
		if (! map.get(a).containsKey(b)) map.put(b, new TreeMap<String, T>());
		map.get(a).get(b).put(c, t);
	}
	
	void write (String a, Map<String, Map<String, T>> m) {
		map.put(a, m);
	}
	
	void write (String a, String b, Map<String, T> m) {
		if (! map.containsKey(a)) map.put(a, new TreeMap<String, TreeMap<String, T>>());
		map.get(a).put(b, m);
	}
	
	// element exists, but it might be null (all we know is the key is there)
	boolean exists (String a, String b, String c) {
		if( map.containsKey(a) && map.get(a).containsKey(b) && map.get(a).get(b).containsKey(c) )
			return true;
		return false;
	}
	
	// element exists + contains something (it's not null)
	boolean defined (String a, String b, String c) {
		if( map.containsKey(a) && map.get(a).containsKey(b) && map.get(a).get(b).containsKey(c) && map.get(a).get(b).get(c)!=null )
			return true;
		return false;
	}
	
	// returns the reference to the object (or null)
	T read (String a, String b, String c) {
		if( map.containsKey(a) && map.get(a).containsKey(b) && map.get(a).get(b).containsKey(c) )
			return map.get(a).get(b).get(c);
		return null;
	}
	
	Set<String> keys () {
		return map.keySet();
	}
	
	Set<String> keys(String a) {
		if (! map.containsKey(a)) map.put(a, new TreeMap<String, TreeMap<String, T>>());
		return map.get(a).keySet();
	}
	
	Set<String> keys(String a, String b) {
		if (! map.containsKey(a)) map.put(a, new TreeMap<String, TreeMap<String, T>>());
		if (! map.get(a).containsKey(b)) map.get(a).put(b, new TreeMap<String, TreeMap<String, T>>());
		return map.get(a).get(b).keySet();
	}
	
	StringTreeMap2D cloned () {
		StringTreeMap2D cloned = new StringTreeMap2D();
		for ( String a : this.keys() ) {
			for ( String b : this.keys(a) ) {
				for ( String c : this.keys(a,b) ) {
					cloned.write( a, b, c, this.read(a,b,c) );
				}
			}
		}
		return cloned;
	}
	
	boolean allNulls () {
		for ( String a : this.keys() ) {
			for ( String b : this.keys(a) ) {
				for ( String c : this.keys(a,b) ) {
					if (map.get(a).get(b).get(c) != null) return false;
				}
			}
		}
		return true;
	}
	
	boolean noNulls () {
		for ( String a : this.keys() ) {
			for ( String b : this.keys(a) ) {
				for ( String c : this.keys(a,b) ) {
					if (map.get(a).get(b).get(c) == null) return false;
				}
			}
		}
		return true;
	}
	
	@Override
	public String toString() { 
		return map.toString(); 
	}
	
}