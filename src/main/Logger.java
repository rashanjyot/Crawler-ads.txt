package main;

import com.sun.istack.internal.NotNull;

import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

public class Logger {

    static final PrintWriter mainPrint = getPrintWriter("res/console.txt");

    public static PrintWriter getPrintWriter(String fileName)
    {
        try
        {
            FileWriter fileWriter = new FileWriter(fileName, true);
            PrintWriter printWriter = new PrintWriter(fileWriter);
            return printWriter;
        }
        catch (IOException e)
        {
            e.printStackTrace();
            return null;
        }
    }

    public static void logDate(@NotNull PrintWriter printWriter)
    {
        DateTimeFormatter dtf = DateTimeFormatter.ofPattern("yyyy/MM/dd HH:mm:ss");
        LocalDateTime now = LocalDateTime.now();
        printWriter.println(dtf.format(now));
    }

    public static synchronized void log(String content)
    {
        try
        {
            PrintWriter printWriter = getPrintWriter("res/printlog.txt");
            logDate(printWriter);
            printWriter.println(content);
            printWriter.close();
        }
        catch (Exception e)
        {
            e.printStackTrace();
        }

    }

    public static void logg(String content)
    {
        try
        {
            mainPrint.println(content);
        }
        catch (Exception e)
        {
            e.printStackTrace();
        }

    }

    public static synchronized void error(Exception e)
    {
        try
        {
            PrintWriter printWriter = getPrintWriter("res/errorLog.txt");
            logDate(printWriter);
            e.printStackTrace(printWriter);
            printWriter.close();
        }
        catch (Exception exception)
        {
            exception.printStackTrace();
        }

    }

    public static synchronized void successLog(String domain, int recordCount)
    {
        try
        {
            PrintWriter printWriter = getPrintWriter("res/successLog.txt");
            printWriter.println(". Domain: "+ domain + " | " + "Records saved: " + recordCount);
            printWriter.close();
        }
        catch (Exception e)
        {
            e.printStackTrace();
        }
    }

    public static synchronized void failureLog(String domain)
    {
        try
        {
            PrintWriter printWriter = getPrintWriter("res/failureLog.txt");
            printWriter.println(". Domain: "+ domain);
            printWriter.close();
        }
        catch (Exception e)
        {
            e.printStackTrace();
        }
    }

    public static synchronized void logCount(int successCount, int failureCount)
    {
        try
        {
            PrintWriter printWriter1 = getPrintWriter("res/successLog.txt");
            printWriter1.println("Count: " + successCount);
            printWriter1.println("---------\n");
            printWriter1.close();

            PrintWriter printWriter2 = getPrintWriter("res/failureLog.txt");
            printWriter2.println("Count: " + failureCount);
            printWriter2.println("---------\n");
            printWriter2.close();
        }
        catch (Exception e)
        {
            e.printStackTrace();
        }
    }

}
