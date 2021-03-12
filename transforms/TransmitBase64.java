package canoedb.transforms;

import java.util.Base64;

public class TransmitBase64 extends Transform {

	// Encode on read
	@Override
	public String onRead ( String s ) {
		byte[] encodedBytes = Base64.getEncoder().encode(s.getBytes());
		return new String(encodedBytes);
	}
	
	// Decode on write
	@Override
	public String onWrite ( String s ) {
		try {
			byte[] decodedBytes = Base64.getDecoder().decode(s.getBytes());
			return new String(decodedBytes);
		} catch (Exception e) {
			System.out.println("TransmitBase64: couldn't decode "+s);
			e.printStackTrace();
			return s;
		}
	}

}