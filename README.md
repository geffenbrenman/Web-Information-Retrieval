# Web-Information-Retrieval
![](cover_image.jpg)
© [All rights reserved.](https://www.ics.uci.edu/~djp3/classes/2009_01_02_INF141/)


* This was an ongoing project as part of the course Web Information Retrieval taught by Professor Sara Cohen. <br>
* A search engine for product reviews. <br>
* All datasets were taken from [Stanford Large Network Dataset Collection](http://snap.stanford.edu/data/index.html).

## Index Writer

Given raw review data, this class creates an on disk index that will <br/>
allow access later on. All data that will be used later on is written to disk in an <br/>
index structure. <br/>
This class also allows an index to be <br/>
removed from disk by deleting all index files. <br/>

### Steps of index building ###
1. Traversing a directory of documents
2. Reading each document and extracting all tokens
3. Computing counts of tokens and documents
4. Building a dictionary of unique tokens in the corpus and their counts
5. Writing to the disk a sorted dictionary


## Index Reader

After an index has been created on disk, the class IndexReader can be used <br/>
to access many different types of information available in the index. <br/>
The operations are implemented in a manner that they are efficient even when the index contains huge <br/>
amounts of data. 


## Review Search

The goal is to search for reviews and for products, based on a specific context, <br/>
and to return these results in ranked order. The implementation includes three different search <br/>
methods, based on three different ranking functions: Vector Space Search, Language Model Search, Product Search.

