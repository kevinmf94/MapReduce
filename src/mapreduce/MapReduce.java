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

import mapreduce.workers.MapWorker;
import mapreduce.workers.ReduceWorker;

public class MapReduce {
	
	private static final int NTHREADS = 4;
	private static final int LINES_TO_READ = 25000;
	
	private List<String> files;
	private Map<String, List<Word>> mapsResults;
	private Class<? extends MapWorker> mapWorker;
	private List<MapWorker> workers = new ArrayList<>();
	private List<Map<String, List<Word>>> allResults;
	
	private ExecutorService threadPool;
	
	public MapReduce(List<String> files, Class<? extends MapWorker> mapWorker) {
		this.files = files;
		this.mapWorker = mapWorker;
		this.allResults = new ArrayList<>();
		initMapWorkers();
	}
	
	private void initMapWorkers() {
		for(int i = 0; i < NTHREADS; i++) {
			try {
				workers.add(mapWorker.newInstance());
			} catch (InstantiationException | IllegalAccessException e) {
				System.out.println("Error worker invalid");
				e.printStackTrace();
			}
		}
	}
	
	private void mapAndShuffle(List<String> linesToProcess) throws InterruptedException, ExecutionException {
		
		List<Future<List<Word>>> futures;
		int blockSize = (linesToProcess.size()/NTHREADS);
		int start, end, i;
		
		//Set workers data and execute maps
		for(i = 0; i < NTHREADS; i++) {
			start = i*blockSize;
			end = i*blockSize+blockSize;
			
			if(i == NTHREADS-1 && linesToProcess.size()%2==1)
				end++;
			
			workers.get(i).setLines(linesToProcess.subList(start, end));
		}
		futures = threadPool.invokeAll(workers);
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
	}
	
	private void reduce() throws InterruptedException, ExecutionException {
		List<Future<Void>> futuresReduces;
		
		//Reduce
		List<Callable<Void>> callables = new ArrayList<Callable<Void>>();
		for (String word : mapsResults.keySet()) {
			callables.add(new ReduceWorker(word, mapsResults));
		}
		futuresReduces = threadPool.invokeAll(callables);
		
		for(int i = 0; i < futuresReduces.size(); i++) {
			futuresReduces.get(i).get();
		}
	}
	
	public String getResultByFile() {
		String result = "";
		
		for(int i = 0; i < allResults.size(); i++) {
			result += String.format("%s:\n", files.get(i));
			Map<String, List<Word>> list = allResults.get(i);
			for (String word : list.keySet()) {
				result += String.format("%s : %d\n", word, list.get(word).get(0).getCount());
			}
		}
		
		return result;
	}
	
	public String getResultsForAll() {
		String result = "";
		Map<String, Integer> finalResult = new TreeMap<>();
		
		for(int i = 0; i < allResults.size(); i++) {
			Map<String, List<Word>> fileRes = allResults.get(i);
			for (String keyWord : fileRes.keySet()) {
				
				if(!finalResult.containsKey(keyWord))
					finalResult.put(keyWord, fileRes.get(keyWord).get(0).getCount());
				else
					finalResult.put(keyWord, finalResult.get(keyWord)+fileRes.get(keyWord).get(0).getCount());
			}
		}
		
		for (String word : finalResult.keySet()) {
			result += String.format("%s : %d\n", word, finalResult.get(word));
		}
		
		return result;
	}
	
	public void execute() {
		threadPool = Executors.newFixedThreadPool(NTHREADS);
		
		for(String file : files) {
			mapsResults = new TreeMap<>();
			
			//Split
			try (BufferedReader br = new BufferedReader(new FileReader(file))) 
			{
				int counter = 1;
				String line = "";
				List<String> linesToProcess = new ArrayList<>();
				
				while(line != null) {

					line = br.readLine();
					if(line != null && !line.equals(""))
						linesToProcess.add(line);
					
					if(counter % LINES_TO_READ == 0 || line == null) {
						mapAndShuffle(linesToProcess);
						reduce();
					}
					
					counter++;
				}
				
			} catch (IOException | InterruptedException | ExecutionException e) {
				System.out.println("Error llegint el fitxer");
				e.printStackTrace();
			}
			
			allResults.add(mapsResults);
		}
		
		threadPool.shutdown();
	}
}
