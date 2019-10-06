import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

public class Logger {
    public static synchronized void log(String content) throws IOException
    {
        try
        {
            DateTimeFormatter dtf = DateTimeFormatter.ofPattern("yyyy/MM/dd HH:mm:ss");
            LocalDateTime now = LocalDateTime.now();

            FileWriter fileWriter = new FileWriter("res/printlog.txt", true);
            PrintWriter printWriter = new PrintWriter(fileWriter);
            printWriter.println(dtf.format(now));
            printWriter.println(content);
            printWriter.close();
        }
        catch (Exception e)
        {
            e.printStackTrace();
        }

    }

    public static synchronized void error(Exception e) throws IOException
    {
        try
        {
            DateTimeFormatter dtf = DateTimeFormatter.ofPattern("yyyy/MM/dd HH:mm:ss");
            LocalDateTime now = LocalDateTime.now();

            FileWriter fileWriter = new FileWriter("res/errorLog.txt", true);
            PrintWriter printWriter = new PrintWriter(fileWriter);
            printWriter.println(dtf.format(now));
            e.printStackTrace(printWriter);
            printWriter.close();
        }
        catch (Exception exception)
        {
            exception.printStackTrace();
        }

    }
}
