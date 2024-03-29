package mapreduce.workers;

import java.util.ArrayList;
import java.util.List;

import mapreduce.Word;

public class MapWorkerByLetter extends MapWorker {

	@Override
	public List<Word> call() throws Exception {
		List<Word> result = new ArrayList<>();
		
		for(String line : getLines()) {
			for(String word : line.split(" ")) {
				for(char letter : word.toCharArray()) {
					result.add(new Word(String.valueOf(letter).toLowerCase(), 1));
				}
			}
		}
		
		return result;
	}
}
