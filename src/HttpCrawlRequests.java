import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URI;
import java.net.URL;
import java.util.ArrayList;
import java.util.Map;
import java.util.regex.Pattern;

public class HttpCrawlRequests {

    public static ArrayList<String[]> getAdsTxtRecords(String domain)
    {
        try
        {
            ArrayList<String[]> recordList = getDataFromRequest("https://"+ domain + "/ads.txt");
            if(recordList != null)
            {
                System.out.println(domain + " count: "+ recordList.size());
            }
            else
            {
                System.out.println(domain + " count: 0");
            }
            return recordList;
        }
        catch (Exception e)
        {
            Logger.error(e);
            return null;
        }
    }

    private static ArrayList<String[]> getDataFromRequest(String url) throws Exception {

        try
        {
            HttpURLConnection con = setupConnection(url);
            System.out.println("\nURL : " + url + " Response Code : "+ con.getResponseCode());
            int responseCode = con.getResponseCode();

            if(responseCode >=300 && responseCode < 400 )
            {
                con = handleRedirects(url, con, 0);
                responseCode = con.getResponseCode();
            }

            if(responseCode >= 200 && responseCode < 300)
            {
                //ignoring all other types for now as it is mentioned in the Ads.txt specification version 1.0.1
                if(con.getContentType().trim().contains("text/plain"))
                {
                    ArrayList<String[]> recordList = parseData(con);
                    return recordList;
                }
                else
                {
                    Logger.log("Content type for " + url + " = " + con.getContentType());
                    return null;
                }

            }
            else if(responseCode >= 400 && responseCode < 600)
            {
              return null;
            }
            return null;

        }
        catch (Exception e) {
            if(url.startsWith("https"))
            {
                return getDataFromRequest(url.replaceFirst("https","http"));
            }
            Logger.log(e.getMessage()+ " ->> " + url);
            return null;
        }
    }

    private static HttpURLConnection handleRedirects(String url, HttpURLConnection con, int hopCount) throws Exception
    {
        String rootDomain =  getRootDomainFromUrl(url);
        String redirectUrl = con.getHeaderField("Location");
        String redirectRootDomain = getRootDomainFromUrl(redirectUrl);

        if(rootDomain.equals(redirectRootDomain) || hopCount++==0)
        {
            HttpURLConnection connection = setupConnection(redirectUrl);

            System.out.println("\nURL : " + redirectUrl + " Response Code : "+ connection.getResponseCode());
            int responseCode = connection.getResponseCode();

            if(responseCode >= 200 && responseCode < 300)
            {
                return connection;
            }
            else if (responseCode >= 300 && responseCode < 400)
            {
                return handleRedirects(connection.getHeaderField("Location"), connection, hopCount);
            }
            return connection;
        }
        else
        {
            throw new RuntimeException("Redirection failed");
        }
    }

    private static HttpURLConnection setupConnection(String url) throws Exception
    {
        URL obj = new URL(url);
        HttpURLConnection connection = (HttpURLConnection) obj.openConnection();
        connection.setInstanceFollowRedirects(true);
        connection.setRequestMethod("GET");
        connection.setRequestProperty("User-Agent", "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_14_6) " +
                "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/77.0.3865.90 Safari/537.36");
        connection.setRequestProperty("Upgrade-Insecure-Requests", "1");
        connection.setConnectTimeout(15000);
        return connection;
    }


    private static String getRootDomainFromUrl(String url) throws Exception
    {
        //extracting rootDomain
        URI uri = new URI(url);
        String rootDomain = uri.getHost();
        rootDomain =  rootDomain.startsWith("www.") ? rootDomain.substring(4) : rootDomain;
        return rootDomain;
    }

    private static ArrayList<String[]> parseData(HttpURLConnection connection) throws Exception
    {
        BufferedReader in = new BufferedReader(new InputStreamReader(connection.getInputStream()));
        String inputLine;

        ArrayList<String[]> recordList = new ArrayList<>();
        while ((inputLine = in.readLine()) != null) {
            try {
                recordList.add(processLineData(inputLine));
            }
            catch (Exception e)
            {
            }
        }
        in.close();
        if(recordList.size()>0)
        {
            return recordList;
        }
        else
        {
            return null;
        }
    }


    private static String[] processLineData(String line) throws RuntimeException
    {
        int commentIndex = line.indexOf("#");
        if(commentIndex != -1)
        {
            line = line.substring(0, commentIndex);
        }
        line = line.trim();
        String[] fields = line.split(",");

        if(!(fields.length == 3 || fields.length == 4))
        {
            throw new RuntimeException("Field count inappropriate-> " + line);
        }

        fields[0] = fields[0].toLowerCase().trim();
        fields[1] = fields[1].trim();
        fields[2] = fields[2].trim().toUpperCase();
        if(fields.length==4)
        {
            fields[3] = fields[3].trim();
        }

        if(!(Pattern.matches("(?:[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?\\.)+[a-z0-9][a-z0-9-]{0,61}[a-z0-9]", fields[0])))
        {
            throw new RuntimeException("Inappropriate advertiser system name-> " + line);
        }

        if(!(fields[2].equals("DIRECT") || fields[2].equals("RESELLER")))
        {
            throw new RuntimeException("Inappropriate Seller account type");
        }
        return fields;

    }

}
