import json
from elasticsearch import Elasticsearch
from elasticsearch_dsl import Search

city_list = []
specialist_list = []
index_name = 'doctors-index-test'
zip_code_list = []

output = {}

client = Elasticsearch()
search = Search(using=client, index=index_name)

for hit in search.scan():
	if hit.city not in city_list:
		city_list.append(hit.city)
	zipcode = hit.office_location.split(' ')[-1]
	if hit.specialties not in specialist_list:
		specialist_list.append(hit.specialties)
	if zipcode not in zip_code_list:
		zip_code_list.append(zipcode)

city_data = {}
for city in city_list:	
	search_city = Search(using=client, index=index_name).query("match", city=city)
	city_data[city] = search_city.count()
output['Total_number_of_doctors_by_city'] = city_data

specialist_data = {}
for specialist in specialist_list:
	search_specialist = Search(using=client, index=index_name).query("match", specialties=specialist)
	specialist_data[specialist] = search_specialist.count()
output['Total_number_of_doctors_by_specialty'] = specialist_data

search_above_20 = Search(using=client, index=index_name).query("match", years_in_practice='21+')
output['20_years_above_experience'] = search_above_20.count()
# print "experience range...(5 - 10 years)"
# search_by_experience = Search(using=client, index=index_name).query("match", years_in_practice='21+')
# print "experience range...(0 - 4 years)"
# print "experience range...(5 - 10 years)"
# print "experience range...(11 - 16 years)"
# print "experience range...(17 - 20 years)"

zipcode_data = {}
for zipcode in zip_code_list:
	search_by_zipcode = Search(using=client, index=index_name).query("match", office_location=zipcode)
	# print "%s : %s" % (zipcode, str(search_by_zipcode.count()))
	zipcode_data[zipcode] = search_by_zipcode.count()
output['Total_number_of_doctors_by_zipcode'] = zipcode_data

print json.dumps(output)