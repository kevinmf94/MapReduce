package mapreduce;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class MapReduceV2 {
	
	private static final int NTHREADS = 4;
	private static final int LINES_TO_READ = 10;
	
	private List<String> files;
	private Map<String, List<Word>> mapsResults;
	private List<Word> result;
	
	public MapReduceV2(List<String> files) {
		this.files = files;
		this.mapsResults = new TreeMap<>();
		this.result = new LinkedList<>();
	}

	public String execute() {
		String result = "";
		
		for(String file : files) {
			result += file+":\n";
			
			ExecutorService pool = Executors.newFixedThreadPool(NTHREADS);
			int counter = 0;
			String line = "";
			
			//Split
			try (BufferedReader br = new BufferedReader(new FileReader(file))) 
			{
				MapWorker map = new MapWorker();
				
				while((line = br.readLine()) != null) {
					map.addLine(line);
					if(counter % LINES_TO_READ == 0) {
						pool.execute(map);
						map = new MapWorker();
					}
					counter++;
				}
				
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			
			System.out.println("Waiting to shutdown");
			pool.shutdown();
			while(!pool.isTerminated());
			
			System.out.println("Start new Pool");
			pool = Executors.newFixedThreadPool(NTHREADS);
			for(String key : mapsResults.keySet()) {
				pool.execute(new ReduceWorker(key, mapsResults.get(key)));
			}
			
			System.out.println("Waiting to shutdown reduce");
			pool.shutdown();
			while(!pool.isTerminated());
			
			for(Word word : this.result)
				result += word.getWord()+" : "+word.getCount()+"\n";
			
		}
		
		return result;
	}
	
	synchronized void addWordShuffle(Word word){
		String wordStr = word.getWord();
		if(!mapsResults.containsKey(wordStr)){
			mapsResults.put(wordStr, new ArrayList<>());
		}
		
		mapsResults.get(wordStr).add(word);
	}

	synchronized void addResult(String wordStr, int count) {
		result.add(new Word(wordStr, count));
	}
	
	class MapWorker implements Runnable {
		
		private static final String SEPARATOR = " ";

		private List<Word> words;
		private List<String> lines;
		
		public MapWorker() {
			lines = new ArrayList<String>();
			words = new ArrayList<Word>();
		}
		
		public void addLine(String line) {
			this.lines.add(line);
		}
		
		@Override
		public void run() {
				
			//Map
			for(String line : lines) {
				for(String word : line.split(SEPARATOR)) {
					word = word.replace("\r", "");
					if(!word.equals(""))
						words.add(new Word(word.toLowerCase(), 1));
				}
			}
			
			//Shuffle
			if(words.size() > 0) {
				for(int i = 0; i < words.size(); i ++) {
					addWordShuffle(words.get(i));
				}
			}
				
		}
		
	}
	
	class ReduceWorker implements Runnable {
	
			private List<Word> words;
			private String wordStr;
			
			public ReduceWorker(String wordStr, List<Word> words) {
				this.wordStr = wordStr;
				this.words = words;
			}
			
			@Override
			public void run() {

				int count = 0;
				for(Word word : words) {
					if(word == null)
						System.out.println("WORD IS NULL "+wordStr);
					
					count += word.getCount();
				}

				addResult(wordStr, count);
			}
			
		}
	
}
