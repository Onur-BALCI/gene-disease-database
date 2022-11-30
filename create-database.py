import string
import time  # in case program freezes add some delay
import psycopg2
import requests
import re
from bs4 import BeautifulSoup

titles = ['Description', 'Frequency', 'Causes']
gene_links, gene_name, normal_function = [], [], []
gc_name, cond_links, gc_desc, freq, causes, cond_relation = [], [], [], [], [], []


def find_div(url, datatype):
    page = requests.get(url)
    soup = BeautifulSoup(page.content, 'html.parser')
    if datatype == 'genes':  # where genes are processed
        results = soup.find_all('li')
        results_text = []
        for r in results[41:][:-16]:
            results_text.append(r.get_text())
        for r in results_text:  # Gene names
            gene_name.append(r)
        gene_link = []
        for r in results[41:][:-16]:  # Gene links
            rl = re.search('<a href="(.*)">', str(r)).group(1)  # finds hyperlinks
            gene_link.append(rl)
            gene_links.append(rl)
        for gene in gene_link:  # Gene descriptions
            page = requests.get(gene)
            soup = BeautifulSoup(page.content, 'html.parser')
            results_exp = soup.find('p').get_text()
            normal_function.append(results_exp)
    if datatype == 'conds':  # where diseases are processed
        results = soup.find_all('li')
        results_text = []
        for r in results[42:][:-16]:
            results_text.append(r.get_text())
        for r in results_text:  # Disease names
            head, sep, tail = r.partition(', see')
            gc_name.append(head)
        for r in results[42:][:-16]:  # Disease links
            rl = re.search('<a href="(.*)">', str(r)).group(1)  # finds hyperlinks
            cond_links.append(rl)
        for y, cond in enumerate(cond_links):
            page = requests.get(cond)
            soup = BeautifulSoup(page.content, 'html.parser')
            results_exp = soup.find_all('div', {"class": "mp-exp exp-full"})[:-4]
            for j, res in enumerate(results_exp):  # Disease descriptions, frequency and causes
                rel = res
                res = res.get_text()
                res = str.replace(res, "\n", "")  # turns to text and strips newlines
                res = str.replace(res, titles[j], "", 1)
                if j == 0:
                    gc_desc.append(res)
                if j == 1:
                    freq.append(res)
                rell = []
                if j == 2:
                    head, sep, tail = res.partition('Learn more about')
                    causes.append(head)
                    for a in rel.find_all('a', href=True):  # Finds what genes cause a disease
                        rell.append(a['href'])
                    for t, r in enumerate(rell):
                        if "https://medlineplus.gov/genetics/gene/" in r:
                            page = requests.get(r)
                            soup = BeautifulSoup(page.content, 'html.parser')
                            r = soup.find('h1', {"class": "with-also"}).get_text()
                            r = (str.replace(r, "\n", ""))
                            r = str.replace(r, " gene", "")
                            cur.execute("select * from genes where gene_name like '%s:%%'" % r)
                            gene = cur.fetchone()
                            if "\\" not in head:
                                head = str.replace(head, "\'", "\\'")
                            cur.execute("select * from gene_genetic_condition where gene_code = E'%s' and gc_code = "
                                        "E'%s'" % (gene[0], "c-" + str(y)))
                            d = cur.fetchone()
                            if d is None:
                                cur.execute("INSERT INTO gene_genetic_condition (gene_code, gc_code, condition_desc)"
                                            " VALUES ('%s', '%s', E'%s')"  # Adds data to the third table
                                            % (gene[0], "c-" + str(y), head))


# connects to database
conn = psycopg2.connect(database="geneticdata", user="postgres", password="admin", host="localhost", port="5432")
cur = conn.cursor()
letters = string.ascii_lowercase
# these values are for testing
p = 0
i_start = 0
i_current = 0
x = 0
for le in letters:
    if letters[p] != "a":
        URL = 'https://medlineplus.gov/genetics/gene-' + letters[p] + '/'
    else:
        URL = 'https://medlineplus.gov/genetics/gene/'
    find_div(URL, 'genes')
    p += 1
    for i, n in enumerate(gene_name):  # verileri veri tabanına işler
        normal_function[i] = str.replace(normal_function[i], "\'", "\\'")
        n = str.replace(n, "\'", "\\'")
        # adds values to database
        cur.execute("select * from genes where gene_name like E'%s'" % n)
        g = cur.fetchone()
        if g is None:
            cur.execute("INSERT INTO genes (gene_code, gene_name, normal_function) VALUES ('%s', E'%s', E'%s')"
                        % ("g-" + str(i + i_start + i_current), n, normal_function[i]))
    i_current += len(gene_name)
    gene_links, gene_name, normal_function = [], [], []
    conn.commit()  # saves data when a page is done
    x += 1
    if x == 100:  # how many pages you want, for testing
        break
p = 0
i_start = 0
i_current = 0
letters = ["0", "a", "b", "c", "d", "e", "f", "g", "h", "i", "j", "k", "l", "m", "n", "o", "p", "q", "r", "s", "t", "u",
           "v", "w", "x", "y", "z"]
x = 0
for le in letters:
    if letters[p] != "a":
        URL = 'https://medlineplus.gov/genetics/condition-' + letters[p] + '/'
    else:
        URL = 'https://medlineplus.gov/genetics/condition/'
    find_div(URL, 'conds')
    p += 1
    conn.commit()
    for i, n in enumerate(gc_name):
        gc_desc[i] = str.replace(gc_desc[i], "\'", "\\'")
        freq[i] = str.replace(freq[i], "\'", "\\'")
        causes[i] = str.replace(causes[i], "\'", "\\'")
        cur.execute("select * from genetic_condition where gc_name like E'%s'" % n)
        g = cur.fetchone()
        if g is None:
            # verileri veri tabanına yazar
            cur.execute("INSERT INTO genetic_condition (gc_code, gc_name, gc_desc, freq, causes)"
                        " VALUES ('%s', E'%s', E'%s', E'%s', E'%s')"
                        % ("c-" + str(i + i_start + i_current), gc_name[i], gc_desc[i], freq[i], causes[i]))
    i_current += len(gc_name)
    gc_name, cond_links, gc_desc, freq, causes, cond_relation = [], [], [], [], [], []
    conn.commit()
    x += 1
    if x == 1:
        break

cur.close()
conn.close()
