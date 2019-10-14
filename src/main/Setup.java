package main;

public class Setup {
    public static final String DB_HOSTNAME = "localhost";
    public static final String DB_PORT = "5432";
    public static final String DB_NAME = "crawlerDb3";
    public static final String DB_USER = "postgres";
    public static final String DB_PASSWORD = "12345";
    public static final String DB_URL = "jdbc:postgresql://" + DB_HOSTNAME + ":" + DB_PORT + "/" + DB_NAME;
    public static final String DB_DRIVER_CLASS = "org.postgresql.Driver";

    public static final String DOMAIN_LIST_FILE = "res/domainList.txt";
}
