import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;

public class Crawler {

    Crawler()
    {

    }

    public void crawlDomain(String domain){
//        HttpCrawlRequests.getAdsTxtForDomain(domain);
    }

    public static void main(String args[])
    {
        BufferedReader reader;
        try {
            int count = 0;
            reader = new BufferedReader(new FileReader(
                    "res/domainList.txt"));
            String line = reader.readLine();
            while (line != null) {
                count++;
                HttpCrawlRequests.getAdsTxtForDomain(line.trim());
                // read next line
                line = reader.readLine();
            }
            System.out.println(count);
            reader.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }


}
