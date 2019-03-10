package mapreduce.workers;

import java.util.ArrayList;
import java.util.List;

import mapreduce.Word;

public class MapWorkerByWord extends MapWorker {

	@Override
	public List<Word> call() throws Exception {
		List<Word> result = new ArrayList<>();
		
		for(String line : getLines()) {
			for(String word : line.split(" ")) {
				word = word.toLowerCase().replace("\r", "");
				result.add(new Word(word, 1));
			}
		}
		
		return result;
	}
	
}
