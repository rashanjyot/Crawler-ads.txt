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

public class Crawler {

    Crawler()
    {

    }

    public void crawlDomain(String domain){
    }

    public static void main(String args[])
    {
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
                ArrayList<String[]> recordList = HttpCrawlRequests.getAdsTxtRecords(domain);
                if(recordList != null){
                    Integer websiteId = saveAndFetchDomainId(domain);
                    HashMap<String, Integer> advertiserNameIdMap = saveAndFetchAdvertiserIds(recordList);
                    boolean saved = saveRecords(websiteId, advertiserNameIdMap, recordList);
                    if(saved)
                    {
                        System.out.println("Saved for: " + domain);
                    }
                    else
                    {
                        System.out.println("Couldn't save for: " + domain);
                    }
                }
                // read next line
                line = reader.readLine();
            }
            reader.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static int saveAndFetchDomainId(@NotNull String domain)
    {
        Connection c = null;
        Statement stmt = null;
        try {
            Class.forName("org.postgresql.Driver");
            c = DriverManager
                    .getConnection("jdbc:postgresql://localhost:5432/crawlerDb",
                            "postgres", "12345");

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
            c.close();

            return websiteId;

        } catch (Exception e) {
            e.printStackTrace();
            Logger.error(e);
            System.exit(0);
            return -1;
        }
    }

    public static HashMap<String,Integer> saveAndFetchAdvertiserIds(@NotNull ArrayList<String[]> recordList)
    {
        Connection c = null;
        Statement stmt = null;
        try {
            Class.forName("org.postgresql.Driver");
            c = DriverManager
                    .getConnection("jdbc:postgresql://localhost:5432/crawlerDb",
                            "postgres", "12345");


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
            c.close();

            return advertiserNameIdMap;

        } catch (Exception e) {
            e.printStackTrace();
            Logger.error(e);
            System.exit(0);
            return null;
        }
    }

    public static boolean saveRecords(Integer websiteId, HashMap<String, Integer> advertiserNameIdMap, ArrayList<String[]> recordList)
    {
        boolean isSaved = false;
        Connection c = null;
        try {
            Class.forName("org.postgresql.Driver");
            c = DriverManager
                    .getConnection("jdbc:postgresql://localhost:5432/crawlerDb",
                            "postgres", "12345");

            c.setAutoCommit(false);
            String insertValues = "";
            for(Integer advertiserId: advertiserNameIdMap.values())
            {
                insertValues += "(" + websiteId + "," + advertiserId + "),";
            }
            insertValues = insertValues.substring(0, insertValues.length() - 1);

            /**
             * TRANSACTION
             * 0. delete where website_id =
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
            isSaved = true;

            c.close();

        } catch (Exception e) {
            e.printStackTrace();
            Logger.error(e);
        }
        finally {
            return isSaved;
        }
    }

}
