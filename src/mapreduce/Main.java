package mapreduce;

import java.util.ArrayList;
import java.util.List;

public class Main {

	public static void main(String[] args) {
		
		String h = "hola\r";
		h.replace("\r", "");

		List<String> files = new ArrayList<>();
		for(String str : args) {
			files.add(str);
		}
		
		long actual = System.currentTimeMillis();
		MapReduce mapReduce = new MapReduce(files);
		String result = mapReduce.execute();
		System.out.println(result);
		long fin = System.currentTimeMillis();
		
		System.out.println("Time: "+(fin-actual));
		
	}

}
