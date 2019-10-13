package main;

import java.io.BufferedReader;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static main.Setup.DOMAIN_LIST_FILE;

public class Crawler {

    private static ExecutorService pool;

    public static void main(String args[])
    {
        pool = Executors.newFixedThreadPool(150);


        BufferedReader reader;
        try {
            reader = new BufferedReader(new FileReader(DOMAIN_LIST_FILE));
            String line = reader.readLine();
            while (line != null) {
                String domain = line.trim();
                /**
                 * Domain is added to pool where each thread makes a HTTP request and then saves to db if ads.txt is fetched
                 *
                 * If dbWrite is successful, domain and recordCount are logged to successLog.txt
                 * else logged to failureLog.txt
                 *
                 * In the end, total count in both the files is logged
                 */
                pool.execute(new Runnable() {
                    @Override
                    public void run() {
                        ArrayList<String[]> recordList = HttpCrawlRequests.getAdsTxtRecords(domain);
                        new DbHelper().save(domain, recordList);
                    }
                });

                // read next line
                line = reader.readLine();
            }
            reader.close();
            pool.shutdown();
            // Long.MAX_VALUE sets for INFINITE WAIT-TIME.
            // refer https://docs.oracle.com/javase/7/docs/api/java/util/concurrent/package-summary.html and search 'MAX_VALUE'
            pool.awaitTermination(Long.MAX_VALUE, TimeUnit.SECONDS);
            DbHelper.printFinalCount();
            System.out.println("All's well that ends well! ;)");
        } catch (Exception e) {
            e.printStackTrace();
            Logger.error(e);
        }
    }

}
