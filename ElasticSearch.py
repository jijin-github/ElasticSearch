import requests
import optparse
import datetime
import time

from Queue import Queue
from threading import Thread
from multiprocessing import Process
from bs4 import BeautifulSoup
from elasticsearch import Elasticsearch


def main():	
	parser = optparse.OptionParser()
	parser.add_option('-i', '--state-name', dest='state_name', default='new-jersey', type='str')
	opts, args = parser.parse_args()
	SearchDoctors(opts.state_name)

class SearchDoctors:
	'''
	Create a report with the physician level details from the New Jersey
	'''
	state_name = None
	city_name = None
	specialist = None
	index_name = 'index-doctors-details'
	threads = []
	process_start_time = None
	num_fetch_threads = 20
	enclosure_queue = Queue()

	def finish_processing(self):
		for process in threads:
			process.join()

	def add_to_elasticsearch(self, doctor_id=None, details={}):
		'''
		Data load into elasticsearch 
		'''
		es = Elasticsearch([{'host': 'localhost', 'port': 9200}])
		es.index(index=self.index_name, doc_type=self.specialist, id=doctor_id, body=details)

	def get_doctor_id(self, doctor_url):
		'''
		Get doctor id from url
		'''
		split_urls = doctor_url.split('/')
		doctor_id = split_urls[-1].split('-')[-1]
		return doctor_id	

	def get_docter_details(self, doctor_id, content):
		'''
		Scrap doctor details
		'''
		details = {}
		soup = BeautifulSoup(content, 'html.parser')		
		details['state'] = self.state_name
		details['city'] = self.city_name
		details['specialties'] = self.specialist
		overview = soup.find('div', {'class': ['block-normal clearfix']})
		if overview:
			details['overview'] = overview.get_text().strip()
		full_name = soup.find('h3', {'class': ['block-loose heading-large']})
		if full_name:	
			details['full_name'] = full_name.get_text().strip()
			print '-------------',self.city_name, self.specialist, full_name.get_text().strip(),"------------------------------------------"
		years_in_practice = soup.find_all('span', {'class': ['text-large heading-normal-for-small-only right-for-medium-up']})
		if years_in_practice:
			details['years_in_practice'] = years_in_practice[1].get_text().strip()
		language = soup.find_all('span', {'class': ['text-large heading-normal-for-small-only right-for-medium-up text-right showmore']})
		if language:
			details['language'] = language[0].get_text().strip()
		office_location = soup.find(attrs={"data-js-id":"doctor-address"})
		if office_location:
			details['office_location'] = office_location.get_text().strip()
		affiliations = []
		for affiliation in soup.find_all('div', {'class': ['search-result-content flex-row flex-ungrid']}):
			affiliations.append(affiliation.text.strip())
		details['hospital_affiliation'] = affiliations
		experience = soup.find(attrs={"data-nav-waypoint":"experience"})
		if experience:
			subspecialties = experience.findChildren('p', {'class': ['text-large block-tight']})
			if subspecialties:
				details['subspecialties'] = subspecialties[0].get_text().strip()		
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
		self.add_to_elasticsearch(doctor_id=doctor_id, details=details)		
		return details		

	def manage_items(self, items):
		'''
		Manage results
		'''
		for item in items:	
			if item.name == 'a':
				if 'Dr.' in item.get_text().strip():
					doctor_name =item.get_text().strip()
					doctor_url = item['href']
					doctor_detail_contents = self.load_url(doctor_url)
					doctor_id = self.get_doctor_id(doctor_url)			
					doctor_details = self.get_docter_details(doctor_id, doctor_detail_contents)					
			else:
				item_name = item.a.get_text().strip()
				item_url = item.a['href']
				self.enclosure_queue.put(item_url)

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
		time.sleep(1.5)
		url = 'https://health.usnews.com'+url
		headers = {'user-agent': 'Mozilla/5.0 (X11; U; Linux i686; en-US; rv:1.8.1.6) Gecko/20070802 SeaMonkey/1.1.4'}
		response = requests.get(url, headers=headers)
		return response.content			

	def get_page_items(self, i, q):
		'''
		Get all the items from the page
		'''
		while True:
			url = q.get()
			result_contents = self.load_url(url)
			url_text = ['specialists-index']
			if any(text in url for text in url_text):
				result_items = self.parse_index_items(result_contents)
				result_items = result_items
			else:
				details_from_url = url.split('/')
				self.city_name = details_from_url[-1]
				self.specialist = details_from_url[-3]
				result_items = self.parse_index_items(result_contents, element='a', class_name='search-result-link')
			self.manage_items(result_items)
			q.task_done()

	def __init__(self, state_name):
		self.state_name = state_name	
		state_url = '/doctors/city-index/'+self.state_name
		state_url_result_contents = self.load_url(state_url)
		# state_result_items = self.parse_index_items(state_url_result_contents)

		soup = BeautifulSoup(state_url_result_contents, 'html.parser')
		state_url_result_contents = soup.find_all('div', {'class': ['flex-small-12 flex-large-6']})[1]
		state_result_items = state_url_result_contents.find_all('li', {'class': ['index-item']})

		
		self.process_start_time = datetime.datetime.now()
		print "Start-----------",datetime.datetime.now()
		for i in range(self.num_fetch_threads):
			worker = Thread(target=self.get_page_items, args=(i, self.enclosure_queue,))
			worker.setDaemon(True)
			worker.start()

		for item in state_result_items:
			item_name = item.a.get_text().strip()
			item_url = item.a['href']
			print item_url,"..........."
			self.enclosure_queue.put(item_url)

		self.enclosure_queue.join()	
		print "End----------",datetime.datetime.now()	

if __name__ == '__main__':
    main()