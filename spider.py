"""
Web crawler with Wikipedia-aware link handling and robots.txt support.

Provenance
----------
The crawler skeleton — the SQLite schema (Pages/Links/Webs), the resumable
crawl loop, and the page-count prompt — comes from the spider.py assignment in
Charles Severance's "Python for Everybody" capstone (py4e.com).

Mine: the Wikipedia link-handling branch, the robots.txt support, and the fix
to the indentation bug that branch originally introduced. See README.md — the
bug is the interesting part, not the crawler.
"""

import sqlite3
import ssl
from urllib.parse import urljoin, urlparse
from urllib.request import urlopen, Request
from urllib.robotparser import RobotFileParser

from bs4 import BeautifulSoup

USER_AGENT = 'search-engine-practice/0.2 (+https://github.com/cbip191/Search-engine)'

# The course code disabled certificate verification unconditionally. That is a
# bad default, so verification is on unless you explicitly turn it off here.
VERIFY_TLS = True

if VERIFY_TLS:
    ctx = ssl.create_default_context()
else:
    ctx = ssl.create_default_context()
    ctx.check_hostname = False
    ctx.verify_mode = ssl.CERT_NONE


# --------------------------------------------------------------------------
# robots.txt
# --------------------------------------------------------------------------

_robots_cache = {}


def can_fetch(url):
    """Return True if robots.txt for this host permits fetching `url`.

    One parser is cached per host. If robots.txt cannot be read we refuse
    rather than proceed — an unreachable policy is not the same as permission.
    """
    parts = urlparse(url)
    root = '{}://{}'.format(parts.scheme, parts.netloc)

    if root not in _robots_cache:
        parser = RobotFileParser()
        parser.set_url(urljoin(root, '/robots.txt'))
        try:
            parser.read()
        except Exception:
            parser = None
        _robots_cache[root] = parser

    parser = _robots_cache[root]
    if parser is None:
        return False
    return parser.can_fetch(USER_AGENT, url)


# --------------------------------------------------------------------------
# Database setup
# --------------------------------------------------------------------------

conn = sqlite3.connect('spider.sqlite')
cur = conn.cursor()

cur.execute('''CREATE TABLE IF NOT EXISTS Pages
    (id INTEGER PRIMARY KEY, url TEXT UNIQUE, html TEXT,
     error INTEGER, old_rank REAL, new_rank REAL)''')

cur.execute('''CREATE TABLE IF NOT EXISTS Links
    (from_id INTEGER, to_id INTEGER,
     PRIMARY KEY(from_id, to_id))''')

cur.execute('''CREATE TABLE IF NOT EXISTS Webs (url TEXT UNIQUE)''')

# Resume an existing crawl if one is in progress.
cur.execute('SELECT id,url FROM Pages WHERE html is NULL and error is NULL ORDER BY RANDOM() LIMIT 1')
row = cur.fetchone()

if row is not None:
    print('Restarting existing crawl. Remove spider.sqlite to start a fresh crawl.')
else:
    starturl = input('Enter web url or enter: ')
    if len(starturl) < 1:
        starturl = 'https://en.wikipedia.org/wiki/Computer_programming'
    if starturl.endswith('/'):
        starturl = starturl[:-1]

    web = starturl
    if starturl.endswith('.htm') or starturl.endswith('.html'):
        pos = starturl.rfind('/')
        web = starturl[:pos]

    if len(web) > 1:
        cur.execute('INSERT OR IGNORE INTO Webs (url) VALUES ( ? )', (web,))
        cur.execute('INSERT OR IGNORE INTO Pages (url, html, new_rank) VALUES ( ?, NULL, 1.0 )',
                    (starturl,))
        conn.commit()

cur.execute('''SELECT url FROM Webs''')
webs = [str(r[0]) for r in cur]

print(webs)


# --------------------------------------------------------------------------
# Crawl loop
# --------------------------------------------------------------------------

many = 0

while True:
    if many < 1:
        sval = input('How many pages:')
        if len(sval) < 1:
            break
        many = int(sval)
    many = many - 1

    cur.execute('SELECT id,url FROM Pages WHERE html is NULL and error is NULL ORDER BY RANDOM() LIMIT 1')
    row = cur.fetchone()
    if row is None:
        print('No unretrieved HTML pages found')
        break

    fromid, url = row[0], row[1]
    print(fromid, url, end=' ')

    if not can_fetch(url):
        print('Disallowed by robots.txt — skipping')
        cur.execute('UPDATE Pages SET error=-2 WHERE url=?', (url,))
        conn.commit()
        continue

    # If we are retrieving this page, there should be no links from it yet.
    cur.execute('DELETE from Links WHERE from_id=?', (fromid,))

    try:
        request = Request(url, headers={'User-Agent': USER_AGENT})
        document = urlopen(request, context=ctx)
        html = document.read()

        if document.getcode() != 200:
            print('Error on page: ', document.getcode())
            cur.execute('UPDATE Pages SET error=? WHERE url=?', (document.getcode(), url))
            conn.commit()
            continue

        if 'text/html' != document.info().get_content_type():
            print('Ignore non text/html page')
            cur.execute('DELETE FROM Pages WHERE url=?', (url,))
            conn.commit()
            continue

        print('(' + str(len(html)) + ')', end=' ')
        soup = BeautifulSoup(html, 'html.parser')

    except KeyboardInterrupt:
        print('')
        print('Program interrupted by user...')
        break

    except Exception as exc:
        print('Unable to retrieve or parse page:', exc)
        cur.execute('UPDATE Pages SET error=-1 WHERE url=?', (url,))
        conn.commit()
        continue

    cur.execute('UPDATE Pages SET html=? WHERE url=?', (memoryview(html), url))
    conn.commit()

    tags = soup('a')
    count = 0

    for tag in tags:
        href = tag.get('href', None)
        if href is None:
            continue

        if href.find('/wiki/') == -1:
            up = urlparse(href)
            if len(up.scheme) < 1:
                href = urljoin(url, href)
                ipos = href.find('#')
                if ipos > 1:
                    href = href[:ipos]
                if href.endswith(('.png', '.jpg', '.gif')):
                    continue
                if href.endswith('/'):
                    href = href[:-1]

            if len(href) < 1:
                continue

            # ------------------------------------------------------------
            # THE FIX. In the previous version, `if not found: continue`
            # and the INSERT below were both indented INTO this loop. A
            # matching host hit `break` first and a non-matching one hit
            # `continue` first, so the INSERT was unreachable on every path
            # and no external link was ever stored. Nothing raised — the
            # crawl simply behaved as though the page had no outbound links.
            # Both statements belong outside the loop. See README.md.
            # ------------------------------------------------------------
            found = False
            for web in webs:
                if href.startswith(web):
                    found = True
                    break

            if not found:
                continue

        else:
            href = 'https://en.wikipedia.org' + href

        cur.execute('INSERT OR IGNORE INTO Pages (url, html, new_rank) VALUES ( ?, NULL, 1.0 )',
                    (href,))
        count = count + 1
        conn.commit()

        cur.execute('SELECT id FROM Pages WHERE url=? LIMIT 1', (href,))
        row = cur.fetchone()
        if row is None:
            print('Could not retrieve id for', href)
            continue

        toid = row[0]
        cur.execute('INSERT OR IGNORE INTO Links (from_id, to_id) VALUES ( ?, ? )', (fromid, toid))

    conn.commit()
    print(count)

cur.close()
conn.close()
