package mapreduce;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class MapReduceV22 {
	
	private static final int NTHREADS = 4;
	private static final int LINES_TO_READ = 2000;
	
	private List<String> files;
	private Map<String, List<Word>> mapsResults;
	
	public MapReduceV22(List<String> files) {
		this.files = files;
		this.mapsResults = new TreeMap<>();
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
				pool.execute(new ReduceWorker(key));
			}
			
			System.out.println("Waiting to shutdown reduce");
			pool.shutdown();
			while(!pool.isTerminated());
			
			for(String key : mapsResults.keySet()) {
				result += key+" : "+mapsResults.get(key).get(0).getCount()+"\n";
			}
			
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
	
	class MapWorker implements Runnable {
		
		private static final String SEPARATOR = " ";

		private List<String> lines;
		private Map<String, List<Word>> words;
		
		public MapWorker() {
			lines = new ArrayList<String>();
			words = new TreeMap<>();
		}
		
		public void addLine(String line) {
			this.lines.add(line);
		}
		
		@Override
		public void run() {
				
			//Map
			for(String line : lines) {
				for(String word : line.split(SEPARATOR)) {
					word = word.replace("\r", "").toLowerCase();
					if(!word.equals("")) {
						
						if(!words.containsKey(word)){
							words.put(word, new ArrayList<>());
						}
						
						words.get(word).add(new Word(word, 1));
					}
						
				}
			}
			
			//Reduce subproblem and shuffle
			int count;
			for(String key : words.keySet()) {
				count = 0;
				for(Word word : words.get(key)) {
					count += word.getCount();
				}
				addWordShuffle(new Word(key, count));
			}
		}
		
	}
	
	class ReduceWorker implements Runnable {
	
			private String wordStr;
			
			public ReduceWorker(String wordStr) {
				this.wordStr = wordStr;
			}
			
			@Override
			public void run() {

				int count = 0;
				for(Word word : mapsResults.get(wordStr)) {
					count += word.getCount();
				}
				
				mapsResults.get(wordStr).clear();
				mapsResults.get(wordStr).add(new Word(wordStr, count));
			}
			
		}
	
}
