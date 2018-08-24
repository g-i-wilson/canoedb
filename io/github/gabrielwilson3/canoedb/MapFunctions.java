package io.github.gabrielwilson3.canoedb;

import java.util.*;

class MapFunctions {

	static Map<String, String> twoStringArrays (String[] keys, String[] values) {
		Map<String, String> aMap = new HashMap<>();
		for (int i = 0; i < keys.length; i++) {
			if (values.length > i && values[i] != null) {
				aMap.put( keys[i], values[i] );
			} else {
				aMap.put( keys[i], "" );
			}
		}
		return aMap;
	}
	
}
