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

    private static Connection c1, c2, c3;

    public static void init()
    {
        c1 = setupConnection();
        c2 = setupConnection();
        c3 = setupConnection();
    }

    public static void save(String domain, ArrayList<String[]> recordList)
    {
        try
        {
            Integer websiteId = saveAndFetchDomainId(c1, domain);
            HashMap<String, Integer> advertiserNameIdMap = saveAndFetchAdvertiserIds(c2, recordList);
            saveRecords(c3, websiteId, advertiserNameIdMap, recordList);
            System.out.println("Saved for: " + domain);
            Logger.successLog(++successCount, domain, recordList.size());
        }
        catch (Exception e)
        {
            Logger.failureLog(++failureCount, domain);
            System.out.println("Couldn't save for: " + domain);
        }
    }

    private static Connection setupConnection()
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

    private static synchronized int saveAndFetchDomainId(Connection c, @NotNull String domain) throws Exception
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

            rs.close();
            stmt.close();
            return websiteId;
        }
        catch (Exception e)
        {
            throw e;
        }
    }

    private static synchronized HashMap<String,Integer> saveAndFetchAdvertiserIds(Connection c, @NotNull ArrayList<String[]> recordList) throws Exception
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

    private static synchronized void saveRecords(Connection c, @NotNull Integer websiteId, @NotNull HashMap<String,
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


    public static void close()
    {
        Logger.logCount(successCount, failureCount);
    }

}
