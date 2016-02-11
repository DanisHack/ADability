from pyspark import SparkConf, SparkContext
from operator import add
import sys, time, os, json
from snakebite.client import Client





''' Batch processing AD events data '''


APP_NAME = "ADability_MVP"

def main(sc, filename):

	''' CALCULATING TOTAL BIDS AND TOTAL VIEWS on ADs for a Company '''

	lines = sc.textFile(filename)
	arr_of_arr = lines.map(lambda line: line.split('\t')).filter(lambda line: len(line) > 3)
	bid_events_arr = arr_of_arr.filter(lambda line: line[0] == 'bid_event')
	view_events_arr = arr_of_arr.filter(lambda line: line[0] == 'view_event').filter(lambda line: line[7] == '1')
	load_events_arr = arr_of_arr.filter(lambda line: line[0] == 'load_event').filter(lambda line: line[7] == '1')
	load_events_arr = arr_of_arr.filter(lambda line: line[0] == 'hover_event').filter(lambda line: line[7] == '1')


	bid_map = bid_events_arr.map(lambda line: (line[2].split('_')[0], 1))
	view_map = view_events_arr.map(lambda line: (line[2].split('_')[0], 1))

	# OUTPUT
	ads_bid_count_by_company = bid_map.reduceByKey(lambda a, b: a + b)
	ads_view_count_by_company = view_map.reduceByKey(lambda a, b: a + b)

	
	print "======== Result =========\n"
	print ads_bid_count_by_company.take(2), ads_view_count_by_company.take(20)
	print "======== Result =========\n"
	ads_bid_count_by_company.saveAsTextFile("hdfs://ec2-52-72-23-2.compute-1.amazonaws.com:9000/user/ubuntu/testdan.txt")
	sc.stop()


if __name__ == "__main__":

	client = Client('ec2-52-72-23-2.compute-1.amazonaws.com', 9000, use_trash=False)
	last_modification_time = sys.argv[1]
	list_of_new_files = [dict for dict in client.ls(['/']) if dict['modification_time'] > last_modification_time]

    # CONFIGURE SPARK
    conf = SparkConf().setAppName(APP_NAME)
    conf = conf.setMaster("local[*]")
    sc = SparkContext(conf=conf)
    
    # FILE TO PROCESS
    filename = sys.argv[1]

    # CALLING MAIN
    main(sc, filename)
