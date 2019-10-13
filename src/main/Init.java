package main;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;

import static main.Setup.*;

public class Init {
    public static void main(String args[]) throws Exception
    {
        Connection connection = setupConnection();
        if(connection != null)
        {
            try {
                connection.setAutoCommit(false);

                // Create tables
                Statement s = connection.createStatement();
                s.execute("CREATE TABLE website(website_id SERIAL PRIMARY KEY,name varchar(100) UNIQUE NOT NULL,last_crawled_at timestamp);");

                s.execute("CREATE TABLE advertiser(advertiser_id SERIAL PRIMARY KEY,name varchar(100) UNIQUE NOT NULL);");

                s.execute("CREATE TABLE publisher(publisher_id SERIAL PRIMARY KEY," +
                        " website_id INTEGER NOT NULL REFERENCES website(website_id) ON DELETE CASCADE," +
                        " advertiser_id INTEGER NOT NULL REFERENCES advertiser(advertiser_id) ON DELETE CASCADE," +
                        " account_id varchar(100) NOT NULL, account_type varchar(200) NOT NULL," +
                        " UNIQUE (website_id, advertiser_id, account_id));");

                //Create indexes
                s.execute("CREATE INDEX ON publisher (advertiser_id);");

                s.execute("CREATE INDEX ON publisher (account_id);");

                connection.commit();
                System.out.println("Created tables and index successfully");
            }
            catch (Exception e)
            {
                connection.rollback();
                System.out.println("Could not create tables and indexes");
            }
            finally {
                connection.close();
            }
        }
        else
        {
            System.out.println("Connection not initialised, please run again!");
        }
    }

    private static Connection setupConnection()
    {
        try
        {
            Class.forName(DB_DRIVER_CLASS);
            Connection c = DriverManager.getConnection(DB_URL, DB_USER, DB_PASSWORD);
            return c;
        }
        catch (Exception e)
        {
            return null;
        }
    }
}
