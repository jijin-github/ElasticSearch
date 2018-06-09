import os
import json
from elasticsearch import Elasticsearch
from elasticsearch_dsl import Search

dir_path = os.path.dirname(os.path.realpath(__file__))

city_list = []
specialist_list = []
index_name = 'index-doctors-details'
zip_code_list = []
# experience range : 0 - 4 years
experience_first = 0
# experience range 5 - 10 years
experience_second = 0
# experience range 11 - 16 years
experience_third = 0
# experience range 17 - 20
experience_fourth = 0
# experience range 20 years above
experience_final = 0

output = {}

client = Elasticsearch()
search = Search(using=client, index=index_name)

for hit in search.scan():
	if hit.city not in city_list:
		city_list.append(hit.city)

	if hasattr(hit, 'office_location'):
		zipcode = hit.office_location.split(' ')[-1]
		if zipcode not in zip_code_list:
			zip_code_list.append(zipcode)
			
	if hit.specialties not in specialist_list:
		specialist_list.append(hit.specialties)

	if hasattr(hit, 'years_in_practice'):
		if '11 - 20' in hit.years_in_practice:
			experience_fourth += 1
			pass
		elif '21+' in hit.years_in_practice:
			experience_final += 1
			pass
		elif '6 - 10' in hit.years_in_practice:
			experience_second += 1
			pass
		elif '3 - 5' in hit.years_in_practice:
			experience_second += 1
			pass
		elif '1 - 2' in hit.years_in_practice:
			experience_first

city_data = {}
for city in city_list:	
	search_city = Search(using=client, index=index_name).query("match", city=city)
	city_data[city] = search_city.count()
output['Total number of doctors by city'] = city_data

specialist_data = {}
for specialist in specialist_list:
	search_specialist = Search(using=client, index=index_name).query("match", specialties=specialist)
	specialist_data[specialist] = search_specialist.count()
output['Total number of doctors by specialty'] = specialist_data

zipcode_data = {}
for zipcode in zip_code_list:
	search_by_zipcode = Search(using=client, index=index_name).query("match", office_location=zipcode)
	zipcode_data[zipcode] = search_by_zipcode.count()
output['Total number of doctors by zipcode'] = zipcode_data

# Total number of doctors based on their experience range
output["experience range...(0 - 4 years)"] = experience_first
output["experience range...(5 - 10 years)"] = experience_second
output["experience range...(11 - 16 years)"] = experience_third
output["experience range...(17 - 20 years)"] = experience_fourth
output["experience range...(17 - 20 years)"] = experience_final

file = open(dir_path+'/results.json', 'w')
file.write(json.dumps(output))
file.close