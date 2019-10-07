package main;

import java.io.BufferedReader;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static main.Setup.DOMAIN_LIST_FILE;

public class Crawler {

    private static ExecutorService dbPool, httpPool;

    public static void main(String args[])
    {
        dbPool = Executors.newFixedThreadPool(50);
        httpPool = Executors.newFixedThreadPool(50);

        DbHelper.init();

        BufferedReader reader;
        try {
            reader = new BufferedReader(new FileReader(DOMAIN_LIST_FILE));
            String line = reader.readLine();
            while (line != null) {
                String domain = line.trim();
                /**
                 * adding domain to httpPool, where a thread will GET data from domain/ads.txt
                 * If succuessful, it will return a recordList to be stored in db, which is added as a task to dbPool
                 *
                 * If dbWrite is successful, domain and recordCount are logged to successLog.txt
                 * else logged to failureLog.txt
                 *
                 * In the end, total count in both the files is logged
                 */
                httpPool.execute(new Runnable() {
                    @Override
                    public void run() {
                        ArrayList<String[]> recordList = HttpCrawlRequests.getAdsTxtRecords(domain);
                        dbPool.execute(new Runnable() {
                            @Override
                            public void run() {
                                DbHelper.save(domain, recordList);
                            }
                        });
                    }
                });

                // read next line
                line = reader.readLine();
            }
            reader.close();
            httpPool.shutdown();
            while (!httpPool.awaitTermination(10, TimeUnit.SECONDS)) {
                System.out.println("Awaiting completion of http pool.");
            }
            dbPool.shutdown();
            while (!dbPool.awaitTermination(10, TimeUnit.SECONDS)) {
                System.out.println("Awaiting completion of db pool.");
            }
            DbHelper.close();
            System.out.println("All's well that ends well! ;)");
        } catch (Exception e) {
            e.printStackTrace();
            Logger.error(e);
        }
    }

}
