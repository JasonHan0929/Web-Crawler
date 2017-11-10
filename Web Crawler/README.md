# Web Crawler 2.0

> Python with BeautifulSoup

## How to Use:

Just open in terminal and type 'python3 crawler.py', then input the three parameters under the instruction, it will ran and get everythin done.
 
The output has two part, one is the pages downloaded in file 'download'. And all the pages are saved in the directory according to their url. Say, url = 'http://www.a.com/b/c.html' will be saved in ./download/www.a.com/b named as 'c.html'. The other part is a file named as 'log.txt' which contains all the log info of the crawler.

The third input paramter, that is the amount of pages you want to crawl, should be larger than 10 as initially, you already have 10 pages as start pages from google

For the key words of query, you could input any string including  a single white space per word. The program could not deal with more than one white space between words.

