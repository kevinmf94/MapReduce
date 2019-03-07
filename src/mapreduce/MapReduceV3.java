package mapreduce;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

public class MapReduceV3 {
	
	private static final int NTHREADS = 8;
	private static final int LINES_TO_READ = 25000;
	private static final int LINES_PER_THREAD = LINES_TO_READ/NTHREADS;
	
	private List<String> files;
	private Map<String, List<Word>> mapsResults;
	
	public MapReduceV3(List<String> files) {
		this.files = files;
		this.mapsResults = new TreeMap<>();
	}

	public String execute() {
		String result = "";
		
		for(String file : files) {
			result += file+":\n";
			
			//Split
			try (BufferedReader br = new BufferedReader(new FileReader(file))) 
			{
				boolean finish = false;
				int counter = 1, i;
				String line = "";
				List<String> linesToProcess = new ArrayList<>();
				ExecutorService pool = Executors.newFixedThreadPool(NTHREADS);
				ExecutorService poolReduces = Executors.newFixedThreadPool(NTHREADS);
				List<MapWorker> workers = new ArrayList<MapWorker>();
				List<Future<List<Word>>> futures;
				List<Future<Void>> futuresReduces;
				
				for(i = 0; i < NTHREADS; i++) {
					workers.add(new MapWorker());
				}
				
				while(!finish) {

					line = br.readLine();
					if(line == null) {
						//Process last items
						break;
					} else {
						if(line.equals(""))
							continue;
						
						linesToProcess.add(line);
						
						if(counter++ % LINES_TO_READ == 0) {
							
							//Set workers and map
							for(i = 0; i < NTHREADS; i++) {
								int start = i*LINES_PER_THREAD;
								int end = i*LINES_PER_THREAD+LINES_PER_THREAD;
								workers.get(i).setLines(linesToProcess.subList(start, end));
							}
							futures = pool.invokeAll(workers);
							linesToProcess.clear();
							
							//Shuffle
							for(i = 0; i < NTHREADS; i++) {
								for(Word word: futures.get(i).get()) {
									String str = word.getWord();
									if(!mapsResults.containsKey(str))
										mapsResults.put(str, new ArrayList<>());
									
									mapsResults.get(str).add(word);
								}
							}
							
							//Reduce
							List<Callable<Void>> callables = new ArrayList<Callable<Void>>();
							for (String word : mapsResults.keySet()) {
								callables.add(new ReduceWorker(word));
							}
							futuresReduces = poolReduces.invokeAll(callables);
							
							for(i = 0; i < futuresReduces.size(); i++) {
								futuresReduces.get(i).get();
							}
						
							
						}
					}
				}
				
				for (String word : mapsResults.keySet()) {
					System.out.println(mapsResults.get(word));
				}
				
				pool.shutdown();
				poolReduces.shutdown();
				
			} catch (IOException | InterruptedException | ExecutionException e) {
				e.printStackTrace();
			}
			
		}
		
		return result;
	}
	
	class MapWorker implements Callable<List<Word>> {

		private List<String> lines;
		private List<Word> result;
		
		public MapWorker() {
			
		}
		
		public void setLines(List<String> lines) {
			this.lines = lines;
			result = new ArrayList<>();
		}
		
		@Override
		public List<Word> call() throws Exception {
			result = new ArrayList<>();
			
			for(String line : lines) {
				for(String word : line.split(" ")) {
					word = word.toLowerCase().replace("\r", "");
					result.add(new Word(word, 1));
				}
			}
			
			return result;
		}
		
	}
	
	class ReduceWorker implements Callable<Void> {
		
		private String wordStr;
		
		public ReduceWorker(String wordStr) {
			this.wordStr = wordStr;
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
}
