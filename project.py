import psycopg2
from pprint import pprint

conn = psycopg2.connect(database='news')
cur = conn.cursor()

# 1. What are the most popular three articles of all time?
articles_query = """ select t1.title, t2.path, t2.count as views from
                      (select slug, title from articles) t1 join
                      (select path, count(path) from log
                      where status = '200 OK' group by path
                      order by count desc) t2
                      on '/article/' || t1.slug = t2.path limit 3"""

cur.execute(articles_query)
articles_list = cur.fetchall()
for title, path, views in articles_list:
    print '{} - {} views'.format(title, views)

# 2. Who are the most popular article authors of all time?
authors_query = """select t3.author, t4.name, t3.sum from
                    (select t1.author, sum(t2.count) from
                    (select author, slug from articles) t1
                    join (select path, count(path) from log
                    where status = '200 OK' group by path
                    order by count desc) t2
                    on '/article/' || t1.slug = t2.path
                    group by t1.author) t3 join
                    (select name, id from authors) t4
                    on t3.author = t4.id order by sum desc
                """
cur.execute(authors_query)
authors_list = cur.fetchall()
for auth_id, author, views in authors_list:
    print '{} - {} views'.format(author, views)

# 3. On which days did more than 1% of requests lead to errors?
requests_query = """select to_char(t1.date, 'FMMonth FMDDth, YYYY'),
                     ((t2.error_requests * 100) / t1.total_request)
                     as percent from ((select date(time), count(time)
                     as total_request from log group by date(time)) t1
                     join (select date(time), count(time) as
                     error_requests from log where status != '200 OK'
                     group by date(time)) t2 on t1.date = t2.date)
                     where ((t2.error_requests * 100) / t1.total_request) > 1
                  """
cur.execute(requests_query)
requests_list = cur.fetchall()
for date, error in requests_list:
    print '{} - {}% errors'.format(date, error)
cur.close()
conn.close()
