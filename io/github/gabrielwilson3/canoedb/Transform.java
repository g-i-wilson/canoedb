package io.github.gabrielwilson3.canoedb;

import java.util.Base64;

class Transform {

	String onRead ( String s ) {
		return s;
	}
	
	String onWrite ( String s ) {
		return s;
	}

}