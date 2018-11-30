package io.github.gabrielwilson3.canoedb;

import java.time.LocalDateTime;

class TimeStamp extends Transform {

	@Override
	String onWrite ( String s ) {
		if (s.equals("auto") || s.equals("AUTO")) {
			return LocalDateTime.now().toString();
		} else {
			return s;
		}
	}

}