import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;

import io.github.gabrielwilson3.canoedb.*;

public class CanoeDB {

	public static void main(String[] args)
	{
		Database d = new Database( args[0] );

		Query q = new Query( d );
		
		q
			.filter( "wagon_instances", "ERA", "AD" )
			.output( "people", "EYE_COLOR" )
			.output( "wagon_types", "FRONT_WHEEL_DIAM" )
			.execute();
			
		System.out.println( "CanoeDB: " + q.map().toString() );
	}
	
}
