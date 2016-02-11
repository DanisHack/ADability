import random, sys, six, json
from datetime import datetime
from kafka.client import KafkaClient
from kafka.producer import KeyedProducer


class DataGenerator(object):

	def __init__(self):
		self.company_name 	= ['housing', 'airbnb', 'spotify', 'ola', 'uber', 'pandora', 'flipkart', 'amazon', 'oyorooms', 'snapdeal']
		self.publisher_name = ["https://www.youtube.com/", "http://www.nytimes.com/", "http://mashable.com/", "http://www.deccanchronicle.com/", "http://economictimes.indiatimes.com/", "https://www.facebook.com/", "https://www.linkedin.com/"]
		self.ad_dimensions 	=  ["320x50", "300x250", "300x50", "250x250", "728x90", "200x200", "468x60", "160x600", "336x280", "120x600", "300x600", "970x90", "320x100", "234x60", "320x480", "120x60","480x320", "970x250", 
		"120x240", "180x150", "250x360", "480x70", "220x90", "300x75", "300x100", "600x300", "125x125", "450x50", "300x1050", "480x32", "980x90", "580x400", "425x600", "960x90", "750x300", "1024x90",
		"970x66", "980x120", "930x180", "240x400", "950x90", "750x100", "750x200", "88x31", "768x1024", "1024x768", "305x64", "300x57", "420x200", "300x60", "216x54", "200x446", "70x70", "192x53", 
		"168x42", "1200x628"]

		#self.output_file = open("./ads_dataset.json", "w")
		self.keyword_file = open("./keywords-dataset-2.txt", "r")

	def random_company_name(self):
		
		return random.choice(self.company_name)

	def random_adability_id(self):
		id = ''.join(random.choice('0123456789ABCDEF') for i in range(16))
		return self.random_company_name()

	def random_campaign_name(self, company):
		id = ''.join(random.choice('56789XYZ'))
		return company+'_campaign'+'_00'+str(random.randint(1,9))

	def random_ad_group_name(self, camp_name):
		return camp_name+'_adgrp_'+self.random_ad_dimension()

	def random_ad_copy_id(self, ad_group):
		return ad_group+'_'+str(random.randint(1,9))

	def random_bid_amount(self):
		return "%.2f" % random.uniform(1, 100)

	def random_publisher(self):
		
		return random.choice(self.publisher_name)

	def random_ad_dimension(self):

		return random.choice(self.ad_dimensions)

	def random_keyword(self):
		self.keyword_file.seek(random.randint(1, 100))
		return self.keyword_file.readline()

	# FUNCTIONS TO GENERATE RANDOM EVENTS

	def bid_event(self):
		''' BID EVENT '''
		curr_time = datetime.now().isoformat()+"Z"
		id = self.random_adability_id()
		camp = self.random_campaign_name(id)
		camp_ad_grp = self.random_ad_group_name(camp)
		camp_ad_copy = self.random_ad_copy_id(camp_ad_grp)

		#print curr_time, id, camp, camp_ad_copy, camp_ad_grp, random_keyword(), random_publisher(), random_bid_amount()

		#bid_str = "bid_event"+"\t"+curr_time+"\t"+id+"\t"+camp+"\t"+camp_ad_grp+"\t"+camp_ad_copy+"\t"+random_keyword()+"\t"+random_publisher()+"\t$"+random_bid_amount()+"\n"
		bid_json = {'event_type':'bid_event', 'time':curr_time, 'company':id, 'campaign':camp, 'adgroup':camp_ad_grp, 'adcopy':camp_ad_copy, 'keyword':self.random_keyword(), 'publisher':self.random_publisher(), 'bid_amount':'$'+self.random_bid_amount()}
		#print type(json.dumps(bid_json))
		#self.output_file.write(json.dumps(bid_json)+'\n')
		return bid_json


	def load_event(self):
		''' LOAD EVENT '''
		curr_time = datetime.now().isoformat()
		id = self.random_adability_id()
		camp = self.random_campaign_name(id)
		camp_ad_grp = self.random_ad_group_name(camp)
		camp_ad_copy = self.random_ad_copy_id(camp_ad_grp)

		#load_event = "load_event"+"\t"+curr_time+"\t"+id+"\t"+camp+"\t"+camp_ad_grp+"\t"+camp_ad_copy+"\t"+random_publisher()+"\t"+str(random.randint(0,2000))+"ms"+"\n"
		load_json = {'event_type':'load_event', 'time':curr_time, 'company':id, 'campaign':camp, 'adgroup':camp_ad_grp, 'adcopy':camp_ad_copy, 'publisher':self.random_publisher(), 'load_time':str(random.randint(0,2000))+"ms"} 
		#self.output_file.write(json.dumps(load_json)+'\n')
		return load_json

	def view_event(self):
		''' VIEW EVENT '''
		curr_time = datetime.now().isoformat()
		id = self.random_adability_id()
		camp = self.random_campaign_name(id)
		camp_ad_grp = self.random_ad_group_name(camp)
		camp_ad_copy = self.random_ad_copy_id(camp_ad_grp)
		#keyword = self.random_keyword()

		#view_event = "view_event"+"\t"+curr_time+"\t"+id+"\t"+camp+"\t"+camp_ad_grp+"\t"+camp_ad_copy+"\t"+random_publisher()+"\t"+str(random.randint(0,1))+"\n"
		view_json = {'event_type':'view_event', 'time':curr_time, 'company':id, 'campaign':camp, 'adgroup':camp_ad_grp, 'adcopy':camp_ad_copy, 'publisher':self.random_publisher(), 'view':str(random.randint(0,1))}
		#self.output_file.write(view_event)
		return view_json

	def hover_event(self):
		''' HOVER EVENT '''
		curr_time = datetime.now().isoformat()
		id = self.random_adability_id()
		camp = self.random_campaign_name(id)
		camp_ad_grp = self.random_ad_group_name(camp)
		camp_ad_copy = self.random_ad_copy_id(camp_ad_grp)

		#hover_event = "hover_event\t"+curr_time+"\t"+id+"\t"+camp+"\t"+camp_ad_grp+"\t"+camp_ad_copy+"\t"+random_publisher()+"\t"+str(random.randint(0,1))+"\n"
		#self.output_file.write(hover_event)
		hover_json = {'event_type':'hover_event', 'time':curr_time, 'company':id, 'campaign':camp, 'adgroup':camp_ad_grp, 'adcopy':camp_ad_copy, 'publisher':self.random_publisher(), 'hover':str(random.randint(0,1))}
		return hover_json

	def click_event(self):
		''' click EVENT '''
		curr_time = datetime.now().isoformat()
		id = self.random_adability_id()
		camp = self.random_campaign_name(id)
		camp_ad_grp = self.random_ad_group_name(camp)
		camp_ad_copy = self.random_ad_copy_id(camp_ad_grp)

		#click_event = "click_event\t"+curr_time+"\t"+id+"\t"+camp+"\t"+camp_ad_grp+"\t"+camp_ad_copy+"\t"+random_publisher()+"\t"+str(random.randint(0,1))+"\n"
		#self.output_file.write(click_event)
		click_json = {'event_type':'click_event', 'time':curr_time, 'company':id, 'campaign':camp, 'adgroup':camp_ad_grp, 'adcopy':camp_ad_copy, 'publisher':self.random_publisher(), 'click':str(random.randint(0,1))}
		return click_json

	def load_error(self):
		pass

class Producer(object):
	
	def __init__(self, addr):
		self.client = KafkaClient(addr)
		self.producer = KeyedProducer(self.client)

	def produce_msgs(self, source_symbol):
		#price_field = random.randint(800,1400)
		msg_cnt = 0

		datagenerator = DataGenerator()

		function_options = {
			0:datagenerator.click_event,
			1:datagenerator.view_event,
			2:datagenerator.bid_event,
			3:datagenerator.hover_event,
			4:datagenerator.load_event
		}

		while True:
			#time_field = datetime.now().strftime("%Y%m%d %H%M%S")
			#price_field += random.randint(-10, 10)/10.0
			#volume_field = random.randint(1, 1000)
			#str_fmt = "{};{};{};{}"
			#message_info = str_fmt.format(source_symbol, time_field, price_field, volume_field)
			num = random.randint(0, 4)
			message_info = function_options[num]()

			print json.dumps(message_info)

			self.producer.send_messages('test_adability', source_symbol, message_info)
			msg_cnt += 1

if __name__ == "__main__":
	args = sys.argv
	ip_addr = str(args[1])
	partition_key = str(args[2])
	prod = Producer(ip_addr)
	prod.produce_msgs(partition_key) 
