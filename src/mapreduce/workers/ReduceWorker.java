package mapreduce.workers;

import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;

import mapreduce.Word;

public class ReduceWorker implements Callable<Void>  {

	private String wordStr;
	private Map<String, List<Word>> mapsResults;
	
	public ReduceWorker(String wordStr, Map<String, List<Word>> mapsResults) {
		this.wordStr = wordStr;
		this.mapsResults = mapsResults;
	}

	@Override
	public Void call() throws Exception {
		int count = 0;
		for(Word word : mapsResults.get(wordStr)) {
			count += word.getCount();
		}
		
		mapsResults.get(wordStr).clear();
		mapsResults.get(wordStr).add(new Word(wordStr, count));
		return null;
	}
		
}