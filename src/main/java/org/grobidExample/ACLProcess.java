package org.grobidExample;

import org.grobid.core.data.BiblioItem;
import org.grobid.core.engines.Engine;
import org.grobid.core.factory.GrobidFactory;
import org.grobid.core.mock.MockContext;
import org.grobid.core.utilities.GrobidProperties;

import java.io.FileInputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.*;

public class ACLProcess {

    private final static int numSecsTimeout = 5;

    private class Worker implements Runnable {
        private final Engine engine;
        private final List<String> pdfPaths;
        private final ExecutorService executor = Executors.newSingleThreadExecutor();
        private long numBytes = 0L;

        public long numProcessed = 0;
        public long totalMillis = 0;
        public long numErrors = 0L;

        public Worker(Engine engine, List<String> pdfPaths) {
            this.engine = engine;
            this.pdfPaths = pdfPaths;
        }

        public void run() {
            List<Future<String>> futures = new ArrayList<Future<String>>();

            engine.processHeader(pdfPaths.get(0), false, new BiblioItem());

            for (String pdfPathLoop : pdfPaths) {
                final String pdfPath = pdfPathLoop;
                Future<String> future = executor.submit(new Callable<String>() {
                    @Override
                    public String call() throws Exception {
                        System.out.println("on " + pdfPath);
                        BiblioItem resHeader = new BiblioItem();
                        return engine.processHeader(pdfPath, false, resHeader);
                    }
                });
                futures.add(future);
            }
            long numBytes = 0;
            try {
                for (Future<String> future : futures) {
                    long start = System.currentTimeMillis();
                    numBytes += future.get(numSecsTimeout, TimeUnit.SECONDS).length();
                    long stop = System.currentTimeMillis();
                    totalMillis += (stop - start);
                    numProcessed++;
                }
            } catch (Exception e) {
                numErrors++;
                e.printStackTrace();
            }
            System.out.println("Num bytes: " + numBytes);
        }
    }


    public ACLProcess(List<String> pdfPaths, int numThreads) throws Exception {
        Properties prop = new Properties();
        prop.load(new FileInputStream("grobid-example.properties"));
        String pGrobidHome = prop.getProperty("grobid_example.pGrobidHome");
        String pGrobidProperties = prop.getProperty("grobid_example.pGrobidProperties");

        MockContext.setInitialContext(pGrobidHome, pGrobidProperties);
        GrobidProperties.getInstance();

        System.out.println(">>>>>>>> GROBID_HOME="+GrobidProperties.get_GROBID_HOME_PATH());

        GrobidFactory grobidFactory = GrobidFactory.getInstance();
        List<Worker> workers = new ArrayList<Worker>(numThreads);
        ExecutorService executorService = Executors.newFixedThreadPool(numThreads);
        int workPerThread = pdfPaths.size() / numThreads;
        for (int idx = 0; idx < numThreads; idx++) {
            int start = idx * workPerThread;
            int end = idx+1 < workers.size() ? (idx+1) * workPerThread : pdfPaths.size();
            Worker w = new Worker(grobidFactory.createEngine(), pdfPaths.subList(start, end));
            executorService.submit(w);
            workers.add(w);
        }
        executorService.shutdown();
        executorService.awaitTermination(numSecsTimeout * pdfPaths.size(), TimeUnit.SECONDS);
        long numProcessed = 0L;
        long totalMillis = 0L;
        long numErrors = 0L;
        for (Worker worker : workers) {
            totalMillis += worker.totalMillis;
            numProcessed += worker.numProcessed;
            numErrors += worker.numErrors;
        }
        double avgMillis = (double) totalMillis / (double) numProcessed;
        System.out.printf("Num processed: %d, num errors: %d, avg millis: %.3f\n", numProcessed, numErrors, avgMillis);
    }

    public static void main(String[] args) throws Exception {
        List<String> pdfPaths = new ArrayList<String>(1000);
        for (int i = 0; i < 1000; i++) {
            pdfPaths.add("/Users/ariah/Desktop/twitter_acl2011.pdf");
        }
        new ACLProcess(pdfPaths, 4);
    }
}
