package canoedb;

import java.util.*;

public class StringMap3D<T> {

	private Map<String, Map<String, Map<String, T>>> map = new LinkedHashMap<>();
	private Set<String> null_set = new LinkedHashSet<>();
	private String hashStr = "";
	private boolean mapChanged = false;

	// access or change internal map object
	public Map<String, Map<String, Map<String, T>>> map () {
		return map;
	}
	public StringMap3D map ( Map<String, Map<String, Map<String, T>>> m ) {
		map = m;
		mapChanged = true;
		return this;
	}
	
	// write
	public StringMap3D write (String a, String b, String c, T t) {
		if (! map.containsKey(a)) map.put(a, new LinkedHashMap<String, Map<String, T>>());
		if (! map.get(a).containsKey(b)) map.get(a).put(b, new LinkedHashMap<String, T>());
		map.get(a).get(b).put(c, t);
		mapChanged = true;
		return this;
	}
	public StringMap3D write (String a, String b, Map<String, T> m) {
		if (! map.containsKey(a)) map.put(a, new LinkedHashMap<String, Map<String, T>>());
		map.get(a).put(b, m);
		mapChanged = true;
		return this;
	}
	public StringMap3D write (String a, Map<String, Map<String, T>> m) {
		map.put(a, m);
		mapChanged = true;
		return this;
	}
	
	// vivify
	public StringMap3D vivify (String a) {
		if (! map.containsKey(a)) map.put(a, new LinkedHashMap<String, Map<String, T>>());
		mapChanged = true;
		return this;
	}
	public StringMap3D vivify (String a, String b) {
		if (! map.containsKey(a)) map.put(a, new LinkedHashMap<String, Map<String, T>>());
		if (! map.get(a).containsKey(b)) map.get(a).put(b, new LinkedHashMap<String, T>());
		mapChanged = true;
		return this;
	}
	public StringMap3D vivify (String a, String b, String c) {
		if (! map.containsKey(a)) map.put(a, new LinkedHashMap<String, Map<String, T>>());
		if (! map.get(a).containsKey(b)) map.get(a).put(b, new LinkedHashMap<String, T>());
		map.get(a).get(b).put(c, null);
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
	public boolean exists (String a, String b, String c) {
		if( map.containsKey(a) && map.get(a).containsKey(b) && map.get(a).get(b).containsKey(c) )
			return true;
		return false;
	}

	// element exists + contains something (it's not null)
	public boolean defined (String a, String b, String c) {
		if( map.containsKey(a) && map.get(a).containsKey(b) && map.get(a).get(b).containsKey(c) && map.get(a).get(b).get(c)!=null )
			return true;
		return false;
	}
	
	// returns the reference to the object (or null)
	public Map<String, Map<String, T>> read (String a) {
		if( map.containsKey(a) )
			return map.get(a);
		return null;
	}
	public Map<String, T> read (String a, String b) {
		if( map.containsKey(a) && map.get(a).containsKey(b) )
			return map.get(a).get(b);
		return null;
	}
	public T read (String a, String b, String c) {
		if( map.containsKey(a) && map.get(a).containsKey(b) && map.get(a).get(b).containsKey(c) )
			return map.get(a).get(b).get(c);
		return null;
	}
	
	public Set<String> keys () {
		return map.keySet();
	}
	
	public Set<String> keys(String a) {
		if (! map.containsKey(a)) return null_set;
		return map.get(a).keySet();
	}
	
	public Set<String> keys(String a, String b) {
		if (! map.containsKey(a)) return null_set;
		if (! map.get(a).containsKey(b)) return null_set;
		return map.get(a).get(b).keySet();
	}
	
	public StringMap3D<T> cloned () {
		StringMap3D<T> cloned = new StringMap3D<>();
		for ( String a : this.keys() ) {
			cloned.vivify(a);
			for ( String b : this.keys(a) ) {
				cloned.vivify(a,b);
				for ( String c : this.keys(a,b) ) {
					cloned.write( a, b, c, this.read(a,b,c) );
				}
			}
		}
		return cloned;
	}
	public StringMap2D<T> cloned (String a) {
		StringMap2D<T> cloned = new StringMap2D<>();
		for ( String b : this.keys(a) ) {
			cloned.vivify(a,b);
			for ( String c : this.keys(a,b) ) {
				cloned.write( b, c, this.read(a,b,c) );
			}
		}
		return cloned;
	}
	public StringMap1D<T> cloned (String a, String b) {
		StringMap1D<T> cloned = new StringMap1D<>();
		for ( String c : this.keys(a,b) ) {
			cloned.write( c, this.read(a,b,c) );
		}
		return cloned;
	}
	
	public StringMap2D<T> referenced (String a) {
		StringMap2D<T> ref = new StringMap2D<>();
		ref.map( read(a) );
		return ref;
	}
	public StringMap1D<T> referenced (String a, String b) {
		StringMap1D<T> ref = new StringMap1D<>();
		ref.map( read(a,b) );
		return ref;
	}

	
	public boolean allNulls () {
		for ( String a : this.keys() ) {
			for ( String b : this.keys(a) ) {
				for ( String c : this.keys(a,b) ) {
					if (map.get(a).get(b).get(c) != null) return false;
				}
			}
		}
		return true;
	}
	
	public boolean noNulls () {
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
				output += b_comma+"\t\t\""+b.toString().replace("\"","\\\"")+"\" : {";
				b_comma = ",\n";
				String c_comma = "\n";
				for ( String c : keys(a,b) ) {
					if (c==null) continue;
					T data = read(a,b,c);
					if (data!=null) {
						output += c_comma+"\t\t\t\""+c.toString().replace("\"","\\\"")+"\" : \""+data.toString().replace("\"","\\\"")+"\"";
					} else {
						output += c_comma+"\t\t\t\""+c.toString().replace("\"","\\\"")+"\" : null";
					}
					c_comma = ",\n";
				}
				output += "\n\t\t}";
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