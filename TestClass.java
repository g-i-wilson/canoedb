
import java.util.*;
import java.io.*;
import java.net.*;

import io.github.gabrielwilson3.canoedb.*;


public class TestClass {
		
	public static void main(String[] args) {
		StringMap3D<String> map = new StringMap3D<String>();
		map.write("I","a","i","1");
		map.write("I","b","i","2");
		map.write("I","c","i","3");
		map.write("I","d","i","4");
		map.write("II","a","i","5");
		map.write("II","b","i","6");
		map.write("II","c","i","7");
		map.write("II","b","ii","8");
		map.write("II","b","iii","9");
		map.write("III","a","i","10");
		
		System.out.println(map);
	}
	
}

