The goal of this programming assignment is to compute the PageRanks of an input set of hyperlinked Wikipedia documents using Spark. The PageRank score of a web page serves as an indicator of the importance of the page. Many web search engines (e.g., Google) use PageRank scores in some form to rank user-submitted queries

Executing PageRankFinal.py on AWS

1) Load all the input files and the source code on the S3 bucket.
2) Create a cluster to run the Spark job
3) Download the input files on AWS and put them on hadoop:
	hadoop fs -mkdir /anagha/Pagerank/Input
	hadoop fs -put /inputfiles /anagha/Pagerank/Input
4) Download the source file PagerankFinal.py on AWS
5) Run the spark job 
	/usr/bin/spark-submit PagerankFinal.py path_to_input_file path_to_output_folder
6) The output for top 100 pageranks and their scores will be displayed on the cmd.
7) Collect the output files by concatenating them and saving them on AWS (local)
	aws s3 cp ./PageRankOutput.txt s3://asarmalkpgtest1/PageRankOutput.txt
8) Download the file into the S3 bucket.
