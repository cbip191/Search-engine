# Search-engine

A small web crawler that stores pages and link structure in SQLite. Wikipedia-aware link
resolution, and it respects `robots.txt`.

This repository is kept for one reason: **it contains a bug I introduced, which passed silently
because the default configuration hid it.** That's the interesting part, and it's documented below.

## Provenance

The crawler skeleton is the `spider.py` assignment from Charles Severance's
[Python for Everybody](https://www.py4e.com/) capstone — the `Pages`/`Links`/`Webs` schema, the
resumable crawl loop, and the page-count prompt are all his.

Mine are the Wikipedia link-handling branch, the `robots.txt` support, and the fix described below.
Stating this plainly because unattributed course code on a public profile is worse than no repo.

## The bug

The course version handles one host set. I added a branch so that Wikipedia's relative `/wiki/`
links resolve against `en.wikipedia.org` while everything else keeps the original
same-host filtering.

The branch was correct. The indentation was not.

**Before:**

```python
found = False
for web in webs:
    if ( href.startswith(web) ) :
        found = True
        break
    if not found : continue

    cur.execute('INSERT OR IGNORE INTO Pages ...')
    count = count + 1
    conn.commit()
```

In the original, `if not found: continue` and the `INSERT` sit *outside* the `for web in webs`
loop. In mine they were indented one level deeper, into it. Trace both paths:

- **Host matches** — `found = True`, then `break`. The loop exits before reaching the `INSERT`.
- **Host doesn't match** — `if not found: continue` fires, advancing the inner loop. Also never
  reaches the `INSERT`.

The `INSERT` was unreachable on every path. No external link was ever written to `Pages`. The
subsequent `SELECT id FROM Pages WHERE url=?` then returned nothing, `row[0]` raised, and a bare
`except` swallowed it into a `Could not retrieve id` line.

**After:**

```python
found = False
for web in webs:
    if href.startswith(web):
        found = True
        break

if not found:
    continue
```

with a single `INSERT` shared by both branches, outside the loop.

## Why it passed

This is the part worth keeping the repo for.

The default start URL is `https://en.wikipedia.org/wiki/Computer_programming`. On Wikipedia,
essentially every internal link contains `/wiki/` — so every link took the *else* branch, which
inserted correctly. The broken path was only reachable via external links, which on that page are
a small minority and produce no visible difference in a crawl summary.

So the failure mode was:

- **No exception.** The bare `except` absorbed the downstream `TypeError`.
- **No wrong-looking output.** The page count incremented and the crawl advanced normally.
- **Masked by the default.** The one configuration anyone would run first was the one that hid it.

A crawl of Wikipedia looked completely healthy. A crawl of anything else silently built a link
graph with no edges — which would then produce a uniform, meaningless PageRank without ever
signalling that anything had gone wrong.

That combination — a plausible result, no error raised, and a default that conceals the failing
path — is exactly what I spend my working time constructing deliberately in evaluation tasks. It
was instructive to find it in my own code, and slightly humbling that it sat there through a
`git push`.

## robots.txt

The course version doesn't check `robots.txt`. This one does, caching one parser per host.

If `robots.txt` can't be read, the crawler **refuses** rather than proceeds. An unreachable policy
is not permission, and defaulting to "allowed" on failure is how well-intentioned crawlers become
badly-behaved ones.

TLS certificate verification is also on by default. The course code disabled it unconditionally;
there's a `VERIFY_TLS` flag if you need the old behaviour, but it shouldn't be the default.

## Usage

```bash
pip install beautifulsoup4
python spider.py
```

You'll be prompted for a start URL (press enter for the Wikipedia default) and then for how many
pages to retrieve per batch. State persists in `spider.sqlite`; delete it to start fresh.

## Known limitations

- **No crawl delay.** Requests go out as fast as the loop runs. `robots.txt` `Crawl-delay` is
  parsed but not honoured. Don't point this at a small site.
- **Full HTML stored as a BLOB** in SQLite. Fine for a few thousand pages, not beyond.
- **`old_rank` / `new_rank` are unused here.** PageRank is computed by a separate program in the
  original course project; this repository contains only the crawler.
- **Random page selection** rather than a proper frontier queue, so crawl order is arbitrary and
  breadth-first behaviour isn't guaranteed.
- **Link extraction is naive** — `<a href>` only. No canonicalisation beyond fragment stripping and
  trailing-slash removal, so the same page reached by different URLs is stored twice.
# Search-engine
Practice search engine with Python
