#!/usr/bin/env python
# coding: utf-8

# In[ ]:


### Anagha Sarmalkar
### 801077504


# In[ ]:


## Import the necessary libraries. The encoding has been set to utf-8 because of Python 2.7 of hadoop
import re
import sys
from pyspark import SparkConf,SparkContext
reload(sys)
sys.setdefaultencoding('utf-8')


# In[ ]:


## Set the application name as PageRank and create a SparkContext
conf = SparkConf().setAppName('PageRank')
sc = SparkContext(conf=conf)


# In[ ]:


#Take the 2nd parameter from commandline as input file
corpus = sc.textFile(sys.argv[1])


# In[ ]:


#Counts the total number of lines in the input file. Initializes the Pagerank of all the pages as 1/total pages
total_pages=corpus.count()
seed_rank = (1/total_pages)


# In[ ]:


#Function to preprocess the data. Titles are filtered out taking title tags into consideration. Links are filtered by retrieving everything in [[]]
def preprocessData(line):
    title = re.findall(r"<title>(.*?)</title>",line)
    link= re.findall(r"\[\[(.*?)\]\]",line)
    return title[0],(seed_rank, link)


# In[ ]:


# Map task to create initial graph link. This has initial pagerank value as seed_rank
map_task1 = corpus.map(lambda line:preprocessData(line))


# In[ ]:


# Function to calculate contribution of every page for other pages
def compute_pr_contri(tup):
    rank = tup[0]
    pages = tup[1]
    len_links = len(pages)
    rank_list=[]
    for link in pages:
        a=tuple((link,rank/len_links))
        rank_list.append(a)
    return rank_list


# In[ ]:


# Function to calculate emit list to carry points-to list through iterations.
def emit_list(x):
    return x[0],x[1][1]


# In[ ]:


# Begin iterations for Pagerank(10)
for pg_iter in range(10):
#     Take the initial graph link rdd and calculate the contributions of all the pages to other pages. Returns a list of contributions for every page
    contrib = map_task1.map(lambda a: compute_pr_contri(a[1]))
#     Unpack this list of contribution tuples
    contribs=contrib.flatMap(lambda a:a)
#     calculate emit list to carry points-to list through iterations.
    url_list = map_task1.map(lambda x: emit_list(x))
#     Add the contributions 
    pr_updated = contribs.reduceByKey(lambda x,y: x+y)
#     Add the damping factor to all the contributions sum
    pr_damp = pr_updated.mapValues(lambda rank: 0.15 + 0.85 * rank)
#     create rdd for next iteration.
    next_list=pr_damp.join(url_list)
    map_task1 = next_list


# In[ ]:


# New RDD to reformat and display output
final_pageranks=map_task1.map(lambda a: (a[0],a[1][0]))


# In[ ]:


# Sort final pageranks by value
final_pageranks=final_pageranks.sortBy(lambda a: -a[1])


# In[ ]:


# Save the final pagerank rdd to a folder specified by the command line parameter 3
final_pageranks.saveAsTextFile(sys.argv[2])


# In[ ]:


# Take the top 100 titles and their pagerank scores
disp_pr=final_pageranks.take(100)


# In[ ]:


# Display the top 100 titles and their pagerank scores
for i in range(100):
    print "TITLE:", disp_pr[i][0],"\t","SCORE:", disp_pr[i][1]

