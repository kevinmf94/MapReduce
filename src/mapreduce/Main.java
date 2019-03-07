package mapreduce;

import java.util.ArrayList;
import java.util.List;

public class Main {

	public static void main(String[] args) {
		
		List<String> files = new ArrayList<>();
		for(String str : args) {
			files.add(str);
		}
		
		long actual = System.currentTimeMillis();
		MapReduceV3 mapReduce = new MapReduceV3(files);
		String result = mapReduce.execute();
		System.out.println(result);
		long fin = System.currentTimeMillis();
		
		System.out.println("Time: "+(fin-actual));
		
	}

}
