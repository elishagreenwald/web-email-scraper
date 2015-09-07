package webemailscraper;

import java.io.IOException;
import java.net.MalformedURLException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;
import org.apache.commons.validator.routines.EmailValidator;
import org.apache.commons.validator.routines.UrlValidator;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;

/**
 * @author Elisha Greenwald
 */
public class WebEmailScraper {

    private final LinkedBlockingQueue<String> linksToScrape;
    private final Set<String> emails;
    private final Set<String> scrapedLinks;
    private final int EMAIL_MAX_COUNT = 1000;
    private final String[] schemes;
    private final UrlValidator urlValidator;
    private final EmailValidator emailValidator;
    private final Pattern p;
    private final ThreadPoolExecutor executorPool;

    public WebEmailScraper() throws InterruptedException {
        long startTime = System.currentTimeMillis();
        this.p = Pattern.compile("[a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\\.[a-zA-Z0-9-]+");
        this.schemes = new String[]{"http", "https"};
        this.emailValidator = EmailValidator.getInstance();
        this.urlValidator = new UrlValidator(schemes);
        this.scrapedLinks = Collections.synchronizedSet(new HashSet<String>());
        this.emails = Collections.synchronizedSet(new HashSet<String>());
        this.linksToScrape = new LinkedBlockingQueue<>();
        this.executorPool = new ThreadPoolExecutor(
                250, 250, 1, TimeUnit.SECONDS, new ArrayBlockingQueue<Runnable>(500));
        executorPool.allowCoreThreadTimeOut(true);
        linksToScrape.add("http://www.cnn.com"); //Starting Link
        scrapedLinks.add("http://www.cnn.com"); 
        int counter = 0;
        String link;

        while (emails.size() < EMAIL_MAX_COUNT) {
            if (executorPool.getQueue().size() < 500) {
                if (!linksToScrape.isEmpty()) {
                    link = linksToScrape.poll();
                    // System.out.println("Email size: " + emails.size());
                    // System.out.println("Scraped Links size: " + scrapedLinks.size());
                    executorPool.submit(new ScraperThread(link, "" + counter++));
                    if (counter % 1000 == 0) {
                        System.out.println(emails.size());
                    }
                } else {
                    System.out.println("Sleeping because no sites in links to scrape");

                    Thread.sleep(1000);

                }
               // System.out.println("links size: " + scrapedLinks.size());
                Thread.sleep(10);

            } else {
                System.out.println("Sleeping because executer queue = " + executorPool.getQueue().size());
                while (executorPool.getQueue().size() > 100) {
                    Thread.sleep(1000);
                    //      System.out.println("Email size: " + emails.size());
                    //         System.out.println("Scraped Links size: " + scrapedLinks.size());

                }
                System.out.println("Waking because executer queue = " + executorPool.getQueue().size());
            }
            //System.out.println(emails.size() + " ");
        }
        executorPool.shutdownNow();

        while (!executorPool.isTerminated()) {
            Thread.sleep(100);

        }
        long endTime = System.currentTimeMillis();
        System.out.println("That took " + (endTime - startTime) + " milliseconds");
        System.out.println(
                "Email size: " + emails.size());
        System.out.println("Total links scraped: " + scrapedLinks.size());
        System.out.println("Links to scrape: " + linksToScrape.size());

      //  System.out.println("");
      //  DatabaseConnect dc = new DatabaseConnect();

    }

    public static void main(String[] args) throws MalformedURLException, IOException, InterruptedException {
        WebEmailScraper finale = new WebEmailScraper();
    }

    private class ScraperThread implements Runnable {

        String url, name;

        public ScraperThread(String url, String name) {
            this.url = url;
            this.name = name;
        }

        @Override
        public void run() {
            
            try {
                Document website = Jsoup.connect(url).get();
                //System.out.println("Thread: " + name + " Scraping " + url);
                Elements scrapedUrls = website.select("a[href]");
                for (Element e : scrapedUrls) {
                    String s = e.attr("abs:href");
                    if (s.contains("linkedin") || s.contains("youtube") || s.contains("google")
                            || s.contains("twitter") || s.contains("vimeo") || s.contains("wikipedia")) {
                        //System.out.println(s  + " caught by blacklist.");
                        scrapedLinks.add(s);
                    } else {
                        if (urlValidator.isValid(s)) {
                            if (scrapedLinks.add(s)) {
                                linksToScrape.add(s);
                                // System.out.println("Added " + s + " to linksToScrape"); 
                            }
                        }
                    }
                }
                Set<String> emailSet = new HashSet<>();
                Elements scrapedEmails = website.getElementsMatchingOwnText("[a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\\.[a-zA-Z0-9-]+");
                for (Element e : scrapedEmails) {
                    if (emailValidator.isValid(e.text())) {
                        System.out.println("Added: " + e.text());
                        emailSet.add(e.text().toLowerCase().trim());
                        
                    }
                }
                emails.addAll(emailSet);
            } catch (IOException e) {
            }
        }
    }

    public class DatabaseConnect {

        public DatabaseConnect() {
            String userName = "", //Set username here
                    password = ""; //Set password here
            // don't forget to add jtds.jar to your classpath or add it to your libraries in NetBeans
            DB db = new DB();
            db.dbConnect(
                    "jdbc:jtds:sqlserver://DATABASEIP:PORT//DATABASENAME", userName, password);//set database ip:port and name here 
            //  "jdbc:jtds:sqlserver://localhost:1433/tempdb","sa","");
        }
    }

    class DB {

        private Connection conn;
        private Statement st;

        public DB() {
        }

        public void dbConnect(String db_connect_string,
                String db_userid, String db_password) {
            try {
                Class.forName("net.sourceforge.jtds.jdbc.Driver");
                conn = DriverManager.getConnection(db_connect_string, db_userid, db_password);
                System.out.println("connected");

                st = conn.createStatement();
                int counter = 0;
                for (String s : emails) {
                    if (counter < 10000) {
                        String t = s.replace("'", "''");
                        System.out.println(t);
                        String insertQuery = String.format("INSERT INTO Emails VALUES ('%s')",
                                t);
                        st.executeUpdate(insertQuery);
                        counter++;
                    }
                }
            } catch (Exception e) {
                e.printStackTrace();
            } finally {
                try {
                    st.close();
                    conn.close();
                } catch (SQLException ex) {

                }

            }
        }
    };

}
//trace statements:
//   System.out.println("Active threads: " + executorPool.getActiveCount());
//       System.out.println("Email size: " + emails.size());
//  System.out.println("Scraped Links size: " + scrapedLinks.size());
//                System.out.println("Thread: " + name + " Scraping " + url);
//                System.out.println("Email size " + emails.size());
