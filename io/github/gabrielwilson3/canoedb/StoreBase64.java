package io.github.gabrielwilson3.canoedb;

import java.util.Base64;

class StoreBase64 extends Transform {

	// Decode on read
	@Override
	String onRead ( String s ) {
		try {
			byte[] decodedBytes = Base64.getDecoder().decode(s.getBytes());
			return new String(decodedBytes);
		} catch (Exception e) {
			System.out.println("StoreBase64: couldn't decode "+s);
			e.printStackTrace();
			return s;
		}
	}
	
	// Encode on write
	@Override
	String onWrite ( String s ) {
		byte[] encodedBytes = Base64.getEncoder().encode(s.getBytes());
		return new String(encodedBytes);
	}

}