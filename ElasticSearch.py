import requests
import optparse
from threading import Thread
import datetime

from bs4 import BeautifulSoup
from elasticsearch import Elasticsearch

es = Elasticsearch([{'host': 'localhost', 'port': 9200}])

def main():	
	parser = optparse.OptionParser()
	parser.add_option('-i', '--state-name', dest='state_name', metavar='<name>', default='new-jersey', 
														type='str', help='The state name to process')
	opts, args = parser.parse_args()
	SearchDoctors(opts.state_name)

class SearchDoctors:
	'''
	Create a report with the physician level details from the New Jersey
	'''
	state_name = None
	city_name = None
	specialist = None
	city_list = []
	specialist_list = []
	number_of_city = 0
	counter = 0
	index_name = 'doctors-index-test'
	threads = []

	def finish_processing(self):
		for process in threads:
			process.join()

	# def create_summary(self):
	# 	'''
	# 	Create summary reporte in json format
	# 	'''
	# 	print "------- Total number of doctors by city ---------"
	# 	for city in self.city_list:
	# 		q = {
	# 			    "query" : {
	# 			        "constant_score" : { 
	# 			            "filter" : {
	# 			                "term" : { 
	# 			                    "city" : city
	# 			                }
	# 			            }
	# 			        }
	# 			    }
	# 			}
	# 		matches = es.search(index=self.index_name, body=q)
	# 		print "%s - %s" % (city, str(len(matches['hits']['hits'])))

	# 	print "Total number of doctors by specialty (element g of the scrapped elements)"
	# 	for specialist in self.specialist_list:
	# 		print specialist,"<<<<<<<<<<<<<<<<"
	# 		q = {
	# 			    "query" : {
	# 			        "constant_score" : { 
	# 			            "filter" : {
	# 			                "term" : { 
	# 			                    "specialties" : specialist
	# 			                }
	# 			            }
	# 			        }
	# 			    }
	# 			}
	# 		matches = es.search(index=self.index_name, body=q)
	# 		print "%s - %s" % (specialist, str(len(matches['hits']['hits'])))

	def add_to_elasticsearch(self, doctor_id=None, details={}):
		'''
		Data load into elasticsearch 
		'''
		print self.city_name, self.specialist, details['full_name'],"<<<<<<<<<<<<<<<<<<<<<<<<<"
		es.index(index=self.index_name, doc_type=self.specialist, id=doctor_id, body=details)

	def get_doctor_id(self, doctor_url):
		'''
		Get doctor id from url
		'''
		split_urls = doctor_url.split('/')
		doctor_id = split_urls[-1].split('-')[-1]
		return doctor_id	

	def get_docter_details(self, content):
		'''
		Scrap doctor details
		'''
		details = {}
		soup = BeautifulSoup(content, 'html.parser')		
		details['state'] = self.state_name
		details['city'] = self.city_name
		if self.city_name not in self.city_list:
			self.city_list.append(self.city_name)	    		
		details['specialties'] = self.specialist
		if self.specialist not in self.specialist_list:
			self.specialist_list.append(self.specialist)
		overview = soup.find('div', {'class': ['block-normal clearfix']})
		if overview:
			details['overview'] = overview.get_text().strip()
		details['full_name'] = soup.find('h3', {'class': ['block-loose heading-large']}).get_text().strip()
		years_in_practice = soup.find_all('span', {'class': ['text-large heading-normal-for-small-only right-for-medium-up']})
		if years_in_practice:
			details['years_in_practice'] = years_in_practice[1].get_text().strip()
		language = soup.find_all('span', {'class': ['text-large heading-normal-for-small-only right-for-medium-up text-right showmore']})
		if language:
			details['language'] = language[0].get_text().strip()
		details['office_location'] = soup.find(attrs={"data-js-id":"doctor-address"}).get_text().strip()
		affiliations = []
		for affiliation in soup.find_all('div', {'class': ['search-result-content flex-row flex-ungrid']}):
			affiliations.append(affiliation.text.strip())
		details['hospital_affiliation'] = affiliations
		experience = soup.find(attrs={"data-nav-waypoint":"experience"})		
		details['subspecialties'] = experience.findChildren('p', {'class': ['text-large block-tight']})[0].get_text().strip()		
		for section_contents in soup.find_all('section', {'class': ['block-loosest']}):
			h2_content = section_contents.find('h2', {'class': ['heading-larger block-normal']})
			if h2_content and h2_content.get_text().strip() == 'Education & Medical Training':
				Education = []
				for education in section_contents.find_all('li', {'class': ['block-tight']}):
					Education.append(education.get_text().strip())
				details['education_and_medical_training'] = Education
			if h2_content and h2_content.get_text().strip() == 'Certifications & Licensure':
				certification_and_licensure = []
				for certification in section_contents.find_all('li', {'class': ['block-tight']}):
					certification_and_licensure.append(certification.get_text().strip())
				details['certification_and_licensure'] = certification_and_licensure
		return details		

	def manage_items(self, items, item_type='specialist_doctors'):
		'''
		Manage results
		'''
		doctor_counter = 0
		if item_type == 'city':
			# items = items[:1]
			self.number_of_city = len(items)
		elif item_type == 'doctor':	
			number_of_doctor = len(items)
		else:
			number_of_doctor = 0
			# items = items[:2]
		for item in items:
			if item_type == 'city':
				self.counter += 1		
			if item.name == 'a':
				if 'Dr.' in item.get_text().strip():
					doctor_counter += 1
					doctor_name =item.get_text().strip()
					doctor_url = item['href']
					doctor_detail_contents = self.load_url(doctor_url)					
					doctor_details = self.get_docter_details(doctor_detail_contents)
					doctor_id = self.get_doctor_id(doctor_url)
					self.add_to_elasticsearch(doctor_id=doctor_id, details=doctor_details)					
					if number_of_doctor == doctor_counter and self.number_of_city == self.counter:
						self.finish_processing()
			else:
				item_name = item.a.get_text().strip()
				item_url = item.a['href']
				self.get_page_items(item_url)


	def parse_index_items(self, content, element='li', class_name='index-item'):
		'''
		Parse site page by 'BeautifulSoup'
		'''
		soup = BeautifulSoup(content, 'html.parser')
		items = soup.find_all(element, {'class': [class_name]})
		return items				

	def load_url(self, url):
		'''
		Load url
		'''
		url = 'https://health.usnews.com'+url
		headers = {'user-agent': 'Mozilla/5.0 (X11; U; Linux i686; en-US; rv:1.8.1.6) Gecko/20070802 SeaMonkey/1.1.4'}
		response = requests.get(url, headers=headers)
		return response.content			

	def get_page_items(self, url, item_type='specialist_doctors'):
		'''
		Get all the items from the page
		'''
		result_contents = self.load_url(url)
		url_text = ['specialists-index', 'city-index']
		if any(text in url for text in url_text):
			result_items = self.parse_index_items(result_contents)			
		else:
			item_type = 'doctor'
			details_from_url = url.split('/')
			self.city_name = details_from_url[-1]
			self.specialist = details_from_url[-3]
			result_items = self.parse_index_items(result_contents, element='a', class_name='search-result-link')
		# self.manage_items(result_items, item_type=item_type)

		process = Thread(target=self.manage_items, args=[result_items], kwargs={'item_type':item_type})
		process.start()
		self.threads.append(process)

	def __init__(self, state_name):
		self.state_name = state_name	
		state_url = '/doctors/city-index/'+self.state_name
		self.get_page_items(state_url, item_type='city')					

if __name__ == '__main__':
    main()