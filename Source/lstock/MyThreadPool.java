package lstock;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

public class MyThreadPool {
	private static final int DEFAULT_MAX_THREAD = 20;
	
	private ExecutorService service;
	private List<Future<?>> futures;
	
	public MyThreadPool() {
		service = Executors.newFixedThreadPool(DEFAULT_MAX_THREAD);
		futures = new ArrayList<>();
	}
	
	public MyThreadPool(int maxThread) {
		service = Executors.newFixedThreadPool(maxThread);
		futures = new ArrayList<>();
	}
	
	public void add(Runnable task) {
		Future<?> f = service.submit(task);
		futures.add(f);
	}
	
	public void waitFinish() {
		// wait for all tasks to complete before continuing
		try {
			for (Future<?> f : futures) {
				f.get();
			}
		} catch (InterruptedException | ExecutionException e) {
			e.printStackTrace();
			System.exit(-1);
		}
		
		service.shutdownNow();
	}
}
