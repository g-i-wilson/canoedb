package io.github.gabrielwilson3.canoedb.transforms;

import java.time.LocalDateTime;
import io.github.gabrielwilson3.canoedb.*;

public class TimeStamp extends Transform {

	@Override
	public String onWrite ( String s ) {
		if (s.toLowerCase().equals("auto")) {
			return LocalDateTime.now().toString();
		} else {
			return s;
		}
	}

}