import com.sun.istack.internal.NotNull;
import sun.rmi.runtime.Log;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class Crawler {

    private static ExecutorService dbPool, httpPool;

    public static Connection setupConnection()
    {
        try
        {
            Class.forName("org.postgresql.Driver");
            Connection c = DriverManager
                    .getConnection("jdbc:postgresql://localhost:5432/crawlerDb",
                            "postgres", "12345");
            return c;
        }
        catch (Exception e)
        {
            return null;
        }
    }

    public static void main(String args[])
    {
        dbPool = Executors.newFixedThreadPool(50);
        httpPool = Executors.newFixedThreadPool(50);
        Connection c1 = setupConnection();
        Connection c2 = setupConnection();
        Connection c3 = setupConnection();

        BufferedReader reader;
        try {
            int count = 0;
            reader = new BufferedReader(new FileReader(
                    "res/domainList2.txt"));
            String line = reader.readLine();
            while (line != null) {
                count++;

                // main logic
                String domain = line.trim();
                httpPool.execute(new Runnable() {
                    @Override
                    public void run() {
                        ArrayList<String[]> recordList = HttpCrawlRequests.getAdsTxtRecords(domain);
                        if(recordList != null){
                            dbPool.execute(new Runnable() {
                                @Override
                                public void run() {
                                    try
                                    {
                                        Integer websiteId = saveAndFetchDomainId(c1, domain);
                                        HashMap<String, Integer> advertiserNameIdMap = saveAndFetchAdvertiserIds(c2, recordList);
                                        saveRecords(c3, websiteId, advertiserNameIdMap, recordList);
                                        System.out.println("Saved for: " + domain);
                                    }
                                    catch (Exception e)
                                    {
                                        e.printStackTrace();
                                        Logger.error(e);
                                        System.out.println("Couldn't save for: " + domain);
                                    }
                                }
                            });
                        }
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

            System.out.println("All's well that ends well! ;)");
        } catch (Exception e) {
            e.printStackTrace();
            Logger.error(e);
        }
    }

    public static synchronized int saveAndFetchDomainId(Connection c, @NotNull String domain) throws Exception
    {
        Statement stmt = null;
        try
        {
            if(c==null || c.isClosed())
            {
                c = setupConnection();
            }
            stmt = c.createStatement();
            ResultSet rs = stmt.executeQuery("INSERT INTO website (name) VALUES ('" + domain + "')  on conflict (name) do nothing returning website_id;");
            int websiteId;
            try {
                rs.next();
                websiteId = rs.getInt("website_id");
            }
            catch (Exception e)
            {
                //exception is raised when conflict happens, run manual select wquery for that
                stmt = c.createStatement();
                rs = stmt.executeQuery("Select website_id from website where name='" + domain + "';");
                rs.next();
                websiteId = rs.getInt("website_id");
            }

            System.out.println(websiteId);

            rs.close();
            stmt.close();
            return websiteId;
        }
        catch (Exception e)
        {
            throw e;
        }
    }

    public static synchronized HashMap<String,Integer> saveAndFetchAdvertiserIds(Connection c, @NotNull ArrayList<String[]> recordList) throws Exception
    {
        Statement stmt = null;
        try
        {
            if(c==null || c.isClosed())
            {
                c = setupConnection();
            }
            String insertValues = "";
            String queryValues = "";
            for(String[] record: recordList)
            {
                insertValues += "('" + record[0] + "'";
                queryValues += "'" + record[0] + "'";
                if (record.length==4)
                {
                    insertValues += ",'" + record[3] + "'";
                }
                else
                {
                    insertValues += ",null";
                }
                insertValues += "),";
                queryValues += ",";
            }
            insertValues = insertValues.substring(0, insertValues.length() - 1);
            queryValues = queryValues.substring(0, queryValues.length() - 1);

            stmt = c.createStatement();
            stmt.execute("INSERT INTO advertiser (name, tag) VALUES " + insertValues +"  on conflict (name) do nothing;");

            stmt = c.createStatement();
            ResultSet rs = stmt.executeQuery("Select advertiser_id, name from advertiser where name IN (" + queryValues + " );");
            HashMap<String,Integer> advertiserNameIdMap = new HashMap<>();
            while (rs.next()) {
                int advertiserId = rs.getInt("advertiser_id");
                String name = rs.getString("name");
                advertiserNameIdMap.put(name, advertiserId);
            }

            rs.close();
            stmt.close();
            return advertiserNameIdMap;
        }
        catch (Exception e)
        {
            throw e;
        }
    }

    public static synchronized void saveRecords(Connection c, @NotNull Integer websiteId, @NotNull HashMap<String,
            Integer> advertiserNameIdMap, @NotNull ArrayList<String[]> recordList) throws Exception
    {
        try
        {
            if(c==null || c.isClosed())
            {
                c = setupConnection();
            }
            c.setAutoCommit(false);

            String insertValues = "";
            for(Integer advertiserId: advertiserNameIdMap.values())
            {
                insertValues += "(" + websiteId + "," + advertiserId + "),";
            }
            insertValues = insertValues.substring(0, insertValues.length() - 1);

            /**
             * TRANSACTION
             * 0. delete where website_id matches (this also deletes from publisher [cascaded])
             * 1. save to website_advertiser_relation
             * 2. save to publisher
             * 3. update last_crawled_at of website
             */

            Statement deleteWebsiteAdvertiserRelation = c.createStatement();
            deleteWebsiteAdvertiserRelation.execute("Delete from website_advertiser_relation where website_id=" + websiteId + "");
            deleteWebsiteAdvertiserRelation.close();

            Statement insertToWebsiteAdvertiserRelation = c.createStatement();
            insertToWebsiteAdvertiserRelation.execute("INSERT INTO website_advertiser_relation (website_id, advertiser_id) VALUES " + insertValues + " returning website_advertiser_relation_id, advertiser_id;");

            ResultSet insertionResultSet = insertToWebsiteAdvertiserRelation.getResultSet();
            HashMap<Integer, Integer> advertiserIdRelationMap = new HashMap<>();
            while(insertionResultSet.next())
            {
                int websiteAdvertiserRelationId = insertionResultSet.getInt("website_advertiser_relation_id");
                int advertiserId = insertionResultSet.getInt("advertiser_id");
                advertiserIdRelationMap.put(advertiserId, websiteAdvertiserRelationId);
                // website id is the same only no need to fetch it
            }
            insertionResultSet.close();
            insertToWebsiteAdvertiserRelation.close();

            insertValues = "";
            for(String[] record: recordList)
            {
                int advId = advertiserNameIdMap.get(record[0]);
                int relationId = advertiserIdRelationMap.get(advId);

                insertValues += "(" + relationId + ",'" + record[1] + "','" + record[2] + "'),";
            }
            insertValues = insertValues.substring(0, insertValues.length() - 1);

            Statement saveToPublisher = c.createStatement();
            saveToPublisher.execute("INSERT INTO publisher (website_advertiser_relation_id, account_id, account_type) " + "VALUES " + insertValues + " on conflict (website_advertiser_relation_id, account_id) do nothing;");
            saveToPublisher.close();

            Statement updateLastCrawledAt = c.createStatement();
            updateLastCrawledAt.execute("UPDATE website set last_crawled_at= now() where website_id=" + websiteId + ";");
            updateLastCrawledAt.close();
            c.commit();
        }
        catch (Exception e)
        {
            c.rollback();
            c.close();
            throw e;
        }
    }

}
