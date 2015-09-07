# web-email-scraper
This java program starts at a user specified website and scrapes email addresses and links to other websites.
Before adding the scraped emails or url's it checks if they were scraped already.
It then continues on to the links it scraped and checks those websites for new emails or url's.
It will continue scraping until it reaches the specified maximum email's to scrape.
When it reaches the maximum emails to scrape it uploads all the email addressess to a specified remote sql server.

Settings to set:

-starting url

-sql server remote ip:port and database name

-sql username and password

Please note the following: 

-Some sites don't like to be scraped and may block your IP permanently or temporariliy from accessing their site.

-The default starting url is cnn.com you can change it to anything though

libraries used (& required for this program to work)

-jsoup-1.8.2 http://jsoup.org/

-jtds-1.2.5 http://sourceforge.net/projects/jtds/files/jtds/1.2.5/

-ApacheRoutines -commons-validator-1.4.1 http://www.apache.org/
