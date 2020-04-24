from bs4 import BeautifulSoup
import sqlite3
import urllib.error
import ssl
from urllib.parse import urljoin
from urllib.parse import urlparse
from urllib.request import urlopen

#retrieve page html
def html_req(url, ctx):
    try:
        document=urlopen(url, context=ctx)
        html=document.read()
        if document.getcode() != 200 :
            print("Error on page: ",document.getcode())
            return('-1')
        if 'text/html' != document.info().get_content_type() :
            print("Ignore non text/html page")
            return('-1')
        return(html)
    except KeyboardInterrupt:
        print('')
        print('Interrupted by user')
        exit()
    except:
        print("Unable to retrieve or parse page")
        return('-1')

#retrieve valid links from page
def retrieve_links(html):
    links=[]
    try:
        soup=BeautifulSoup(html,'html.parser')
        #print(soup.prettify)
        tags=soup('a')
        for tag in tags:
            #print(tag)
            href=tag.get('href',None)
            if href is None: continue
            elif href.find('http') != -1 : links.append(href)   #Checking whether tag contains valit http link
            elif href[:6]=='/wiki/' and href.find(':')==-1:  #Creating valid links to href of shape /wiki/...
                href='https://en.wikipedia.org'+href
                links.append(href)
                #print(href)
        return(links)
    except:
        print ('An error occured. Not valid HTML')

def database_fullf(): input()

#cleaning Webs database from wrong inputs
def webs_cleanup(cur, conn):
    cur.execute('''SELECT COUNT(url) from Webs''')
    len_db=int(cur.fetchone()[0])
    cur.execute('SELECT * from Webs')
    tup_urls=cur.fetchall()
    conn.commit()

#obtain domain between http and first / after domain
def receive_domain(link):
    pos=link.find('/')
    start=link[:pos+2]
    link=link[pos+2:]
    #print(link)
    pos=link.find('/')
    link=start+link[:pos]
    #print(link)
    return(link)

#identifies whether link belongs to the domain from Webs base
def link_belongs_to_list(cur, conn, link):
    cur.execute('SELECT url from Webs')
    t_list_urls=cur.fetchall()
    list_urls=[]
    for t_url in t_list_urls:   list_urls.append(t_url[0])
    if receive_domain(link) in list_urls:   return(True)
    else:
        return(False)

#shortening list of links to ones that are within websites from DB Webs
def links_cleaning(cur, conn, links_longlist):
    links_list=[]
    for link in links_longlist:
        if link_belongs_to_list(cur, conn, link):   links_list.append(link)
    return(links_list)

#checking if link does not work
def broken_link_db(cur, conn, url):
    #cur.execute('UPDATE Pages SET error=0 WHERE url=(?)',(url,))
    #conn.commit()
    cur.execute('SELECT url FROM Pages WHERE HTML IS NULL AND error<>0 GROUP BY RANDOM() LIMIT 1')
    if cur.fetchone() is None:
        print('Check connection, then drop database or add working URL to Pages and restart the process with another URL')
        exit()
    return(cur.fetchone()[0])

#testing whether DB contains valid links
def start_check(cur, conn):
    cur.execute('SELECT url FROM Pages WHERE html IS NULL GROUP BY RANDOM() LIMIT 1')
    url=cur.fetchone()
    #print(type(url),url)
    #print(type(cur.fetchone()), cur.fetchone())
    if url is None:
        #print(cur.fetchone())
        return(True)
    else:
        return(False)

#input new urls and links to DB
def db_links_input(cur, conn, links_list, from_id):
    for link in links_list:
        cur.execute('INSERT OR IGNORE INTO Pages(url, new_rank) VALUES(?,?)',(link,1.0))
        conn.commit()
        cur.execute('SELECT id FROM Pages WHERE url=?',(link,))
        to_id=cur.fetchone()[0]
        cur.execute('INSERT OR IGNORE INTO Links(from_id, to_id) VALUES(?,?)',(from_id,to_id))
        conn.commit()

def main():

    # Ignore SSL certificate errors
    ctx = ssl.create_default_context()
    ctx.check_hostname = False
    ctx.verify_mode = ssl.CERT_NONE

    #create database if needed
    conn = sqlite3.connect('newspider.sqlite')
    cur = conn.cursor()

    cur.execute('''CREATE TABLE IF NOT EXISTS Pages
        (id INTEGER PRIMARY KEY, url TEXT UNIQUE, html TEXT,
         error INTEGER, old_rank REAL, new_rank REAL)''')

    cur.execute('''CREATE TABLE IF NOT EXISTS Links
        (from_id INTEGER, to_id INTEGER,
        PRIMARY KEY(from_id, to_id))''')

    cur.execute('''CREATE TABLE IF NOT EXISTS Webs (url TEXT UNIQUE)''')
    #end of database creation

    webs_cleanup(cur, conn)

#Set beginning url, insert it into DB
    #print(start_check(cur, conn))
    if start_check(cur, conn) is True:
        start_url=input ('Enter url:')
        if len(start_url)<1:    start_url='https://en.wikipedia.org/wiki/Computer_programming'
        cur.execute('INSERT OR IGNORE INTO Webs(url) VALUES (?)',(receive_domain(start_url),))
        conn.commit()

        #retrieve HTML of the starting page, insert it into db
        html = html_req(start_url, ctx)
        if html!='-1':
            cur.execute('INSERT OR IGNORE INTO Pages(url, html, new_rank) VALUES (?,?,?)',(start_url,html,1.0))
            conn.commit()
        else:
            broken_link_db(cur, conn, start_url)

            #identify id of the starting page to set links
        cur.execute('SELECT id FROM Pages WHERE url=?',(start_url,))
        from_id=cur.fetchone()[0]
        links_longlist=retrieve_links(html)
        links_list=links_cleaning(cur,conn,links_longlist)

        db_links_input(cur, conn, links_list, from_id)
    else:
        #Spidering for given number of times
        counter=int(input('How many pages? '))

        while counter>0:
            cur.execute('SELECT url FROM Pages WHERE html IS NULL GROUP BY RANDOM () LIMIT 1')
            url=cur.fetchone()[0]

            html='-1'
            while html=='-1':   #passing pages that don't work
                html = html_req(url, ctx)
                if html=='-1':
                    url = broken_link_db(cur, conn,url)

            cur.execute('UPDATE Pages SET html=? WHERE url=?',(html,url))
            conn.commit()
            links_longlist=retrieve_links(html)
            links_list=links_cleaning(cur,conn,links_longlist)
            cur.execute('SELECT id FROM Pages WHERE url=?',(url,))
            from_id=cur.fetchone()[0]
            db_links_input(cur, conn, links_list, from_id)
            counter-=1
main()
