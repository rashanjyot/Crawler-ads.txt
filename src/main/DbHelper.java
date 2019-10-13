package main;

import com.sun.istack.internal.NotNull;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;

import static main.Setup.*;

public class DbHelper {

    private static int successCount = 0, failureCount = 0;

    private Connection c;

    DbHelper()
    {
        c = setupConnection();
    }

    public synchronized static void incrementSuccess()
    {
        successCount++;
    }

    public synchronized static void incrementFailure()
    {
        failureCount++;
    }

    public synchronized static void printError(Exception e, String domain)
    {
        System.err.println(domain);
        e.printStackTrace();
    }

    public synchronized void save(String domain, ArrayList<String[]> recordList)
    {
        try
        {
            Integer websiteId = saveAndFetchDomainId(c, domain);
            HashMap<String, Integer> advertiserNameIdMap = saveAndFetchAdvertiserIds(c, recordList);
            saveRecords(c, websiteId, advertiserNameIdMap, recordList);
            incrementSuccess();
            Logger.successLog(domain, recordList.size());
            System.out.println("Saved for: " + domain);
        }
        catch (Exception e)
        {
            printError(e, domain);
            incrementFailure();
            Logger.failureLog(domain);
            System.out.println("Couldn't save for: " + domain);
        }
    }

    private synchronized Connection setupConnection()
    {
        try
        {
            Class.forName(DB_DRIVER_CLASS);
            Connection c = DriverManager
                    .getConnection(DB_URL, DB_USER, DB_PASSWORD);
            return c;
        }
        catch (Exception e)
        {
            return null;
        }
    }

    private synchronized int saveAndFetchDomainId(Connection c, @NotNull String domain) throws Exception
    {
        Logger.inOutLog("IN: "+Thread.currentThread().getName());
        Statement stmt = null;
        try
        {
            if(c==null || c.isClosed())
            {
                c = setupConnection();
            }
            c.setAutoCommit(true);
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
            Logger.inOutLog("OUT: "+Thread.currentThread().getName());
            rs.close();
            stmt.close();
            return websiteId;
        }
        catch (Exception e)
        {
            throw e;
        }
    }

    private synchronized HashMap<String,Integer> saveAndFetchAdvertiserIds(Connection c, @NotNull ArrayList<String[]> recordList) throws Exception
    {
        Statement stmt = null;
        try
        {
            if(c==null || c.isClosed())
            {
                c = setupConnection();
            }
            c.setAutoCommit(true);
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

    private synchronized void saveRecords(Connection c, @NotNull Integer websiteId, @NotNull HashMap<String,
            Integer> advertiserNameIdMap, @NotNull ArrayList<String[]> recordList) throws Exception
    {
        try
        {
            if(c==null || c.isClosed())
            {
                c = setupConnection();
            }
            c.setAutoCommit(false);


            /**
             * TRANSACTION
             * 1. delete from publisher where website_id matches
             * 2. save to publisher
             * 3. update last_crawled_at of website
             */

            Statement deletePublisherForWebsite = c.createStatement();
            deletePublisherForWebsite.execute("Delete from publisher where website_id=" + websiteId + "");
            deletePublisherForWebsite.close();

            String insertValues = "";
            for(String[] record: recordList)
            {
                int advId = advertiserNameIdMap.get(record[0]);
                insertValues += "(" + websiteId + "," + advId + ",'" + record[1] + "','" + record[2] + "'),";
            }
            insertValues = insertValues.substring(0, insertValues.length() - 1);

            Statement saveToPublisher = c.createStatement();
            saveToPublisher.execute("INSERT INTO publisher (website_id, advertiser_id, account_id, account_type) " + "VALUES " + insertValues + " on conflict (website_id, advertiser_id, account_id) do nothing;");
            saveToPublisher.close();

            Statement updateLastCrawledAt = c.createStatement();
            updateLastCrawledAt.execute("UPDATE website set last_crawled_at= now() where website_id=" + websiteId + ";");
            updateLastCrawledAt.close();
            c.commit();
            c.close();
        }
        catch (Exception e)
        {
            c.rollback();
            c.close();
            throw e;
        }
    }


    public static void printFinalCount()
    {
        Logger.logCount(successCount, failureCount);
    }

}
