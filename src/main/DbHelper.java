package main;

import com.sun.istack.internal.NotNull;

import java.sql.*;
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

    public synchronized static void incrementSuccessCount()
    {
        successCount++;
    }

    public synchronized static void incrementFailureCount()
    {
        failureCount++;
    }

    public synchronized void save(String domain, ArrayList<String[]> recordList)
    {
        try
        {
            if(recordList==null) throw new RuntimeException("Recordlist is null");

            Integer websiteId = saveAndFetchDomainId(c, domain);
            HashMap<String, Integer> advertiserNameIdMap = saveAndFetchAdvertiserIds(c, recordList);
            saveRecords(c, websiteId, advertiserNameIdMap, recordList);
            incrementSuccessCount();
            Logger.successLog(domain, recordList.size());
            System.out.println("Saved for: " + domain);
            Logger.logg("Saved for: " + domain);

        }
        catch (Exception e)
        {
            incrementFailureCount();
            Logger.failureLog(domain);
            System.out.println("Couldn't save for: " + domain);
            Logger.logg("Couldn't save for: " + domain);
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
        try
        {
            if(c==null || c.isClosed())
            {
                c = setupConnection();
            }
            c.setAutoCommit(true);
            PreparedStatement ps = c.prepareStatement("INSERT INTO website (name) VALUES (?)  on conflict (name) do nothing returning website_id;");
            ps.setString(1, domain);
            ResultSet rs = ps.executeQuery();
            int websiteId;
            try {
                rs.next();
                websiteId = rs.getInt("website_id");
            }
            catch (Exception e)
            {
                //exception is raised when conflict happens, run manual select wquery for that
                ps = c.prepareStatement("Select website_id from website where name=?;");
                ps.setString(1, domain);
                rs = ps.executeQuery();
                rs.next();
                websiteId = rs.getInt("website_id");
            }
            rs.close();
            ps.close();
            return websiteId;
        }
        catch (Exception e)
        {
            throw e;
        }
    }

    private synchronized HashMap<String,Integer> saveAndFetchAdvertiserIds(Connection c, @NotNull ArrayList<String[]> recordList) throws Exception
    {
        try
        {
            if(c==null || c.isClosed())
            {
                c = setupConnection();
            }
            c.setAutoCommit(true);
            String queryValues = "";
            PreparedStatement ps = c.prepareStatement("INSERT INTO advertiser (name) VALUES (?) on conflict (name) do nothing;");
            for(String[] record: recordList)
            {
                ps.setString(1, record[0]);
                ps.addBatch();
                queryValues += "'" + record[0] + "'";
                queryValues += ",";
            }
            queryValues = queryValues.substring(0, queryValues.length() - 1);
            ps.executeBatch();
            ps.close();

            ps = c.prepareStatement("Select advertiser_id, name from advertiser where name IN ("+ queryValues +");");
            ResultSet rs = ps.executeQuery();
            HashMap<String,Integer> advertiserNameIdMap = new HashMap<>();
            while (rs.next()) {
                int advertiserId = rs.getInt("advertiser_id");
                String name = rs.getString("name");
                advertiserNameIdMap.put(name, advertiserId);
            }

            rs.close();
            ps.close();
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

            PreparedStatement deletePublisherForWebsite = c.prepareStatement("Delete from publisher where website_id = ?;");
            deletePublisherForWebsite.setInt(1, websiteId);
            deletePublisherForWebsite.execute();
            deletePublisherForWebsite.close();

            PreparedStatement saveToPublisher = c.prepareStatement("INSERT INTO publisher (website_id, advertiser_id, account_id, account_type) VALUES (?,?,?,?) on conflict (website_id, advertiser_id, account_id) do nothing;");
            for(String[] record: recordList)
            {
                int advId = advertiserNameIdMap.get(record[0]);
                saveToPublisher.setInt(1, websiteId);
                saveToPublisher.setInt(2, advId);
                saveToPublisher.setString(3, record[1]);
                saveToPublisher.setString(4, record[2]);
                saveToPublisher.addBatch();
            }
            saveToPublisher.executeBatch();
            saveToPublisher.close();

            PreparedStatement updateLastCrawledAt = c.prepareStatement("UPDATE website set last_crawled_at= now() where website_id = ?;");
            updateLastCrawledAt.setInt(1, websiteId);
            updateLastCrawledAt.execute();
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
