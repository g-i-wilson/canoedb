package io.github.gabrielwilson3.canoedb;

import java.time.LocalDateTime;

class TimeStamp extends Transform {

	@Override
	String onWrite ( String s ) {
		if (s.toLowerCase().equals("auto")) {
			return LocalDateTime.now().toString();
		} else {
			return s;
		}
	}

}