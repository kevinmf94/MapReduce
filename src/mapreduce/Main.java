package mapreduce;

import java.util.ArrayList;
import java.util.List;

import mapreduce.workers.MapWorkerByLetter;

public class Main {
	
	private static final int NTHREADS = 4;
	private static final int LINES_TO_READ = 25000;

	public static void main(String[] args) {
		
		boolean histogramByLetter = false;
		boolean histogramByFile = true;
		boolean printTime = false;
		int nThreads = NTHREADS;
		int blockLinesSplit = LINES_TO_READ;
		
		List<String> files = new ArrayList<>();
		for(int i = 0; i < args.length; i++) {
			if(args[i].equals("-l")) {//Activate histogram by letter
				histogramByLetter = true;
				continue;
			}
			else if(args[i].equals("-a")) {// activate histogram for all files
				histogramByFile = false;
				continue;
			}
			else if(args[i].equals("-t")) {// Configure threads used
				nThreads = Integer.valueOf(args[++i]);
				continue;
			}
			else if(args[i].equals("-b")) {//Configure blocks of lines
				blockLinesSplit = Integer.valueOf(args[++i]);
				continue;
			}
			else if(args[i].equals("-s")) {//Activate print time
				printTime = true;
				continue;
			}

			files.add(args[i]);
		}
		
		//Comprovaci� de casos erronis.
		if(nThreads < 1) {
			System.err.println("Numero de threads invalid");
			return;
		}
		
		if(blockLinesSplit < 1) {
			System.err.println("El numero de linies per bloc a de ser un nombre positiu");
			return;
		}
		
		if(nThreads > blockLinesSplit) {
			System.err.println("Les linies per bloc tenen que ser majors que el numero de threads");
			return;
		}
		
		if(files.size() == 0) {
			System.err.println("No s'ha introduit cap fitxer");
			return;
		}
		
		long actual = System.currentTimeMillis();
		
		MapReduce mapReduce;
		if(histogramByLetter)
			mapReduce = new MapReduce(files, nThreads, blockLinesSplit, MapWorkerByLetter.class);
		else
			mapReduce = new MapReduce(files, nThreads, blockLinesSplit);
		
		mapReduce.execute();
		
		if(histogramByFile)
			System.out.println(mapReduce.getResultByFile());
		else
			System.out.print(mapReduce.getResultsForAll());
		
		long fin = System.currentTimeMillis();
		if(printTime)
			System.out.println("Time: "+(fin-actual));
		
	}

}
