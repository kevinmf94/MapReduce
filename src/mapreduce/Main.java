package mapreduce;

import java.util.ArrayList;
import java.util.List;

import mapreduce.workers.MapWorkerByLetter;
import mapreduce.workers.MapWorkerByWord;

public class Main {
	

	public static void main(String[] args) {
		
		boolean byLetter = false;
		boolean allFilesHisto = false;
		
		List<String> files = new ArrayList<>();
		for(String str : args) {
			if(str.equals("-l")) {
				byLetter = true;
				continue;
			}
			if(str.equals("-a")) {
				allFilesHisto = true;
				continue;
			}
			files.add(str);
		}
		
		long actual = System.currentTimeMillis();
		MapReduce mapReduce;
		if(byLetter)
			mapReduce = new MapReduce(files, MapWorkerByLetter.class);
		else
			mapReduce = new MapReduce(files, MapWorkerByWord.class);
		
		mapReduce.execute();
		
		if(allFilesHisto)
			System.out.print(mapReduce.getResultsForAll());
		else
			System.out.println(mapReduce.getResultByFile());
		
		long fin = System.currentTimeMillis();
		
		//System.out.println("Time: "+(fin-actual));
		
	}

}
