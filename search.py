# import requests
# import json

# from elasticsearch import Elasticsearch
# es = Elasticsearch([{'host': 'localhost', 'port': 9200}])
# # q = {"query": {"match": {"full_name": "Dr. Alexander Axelrad's Details"}}}
# q2 = {
#     "query": {
#         "query_string" : {
#             "default_field" : "full_name",
#             "query" : "absecon"
#         }
#     }
# }

# q = {
# 	    "query" : {
# 	        "constant_score" : { 
# 	            "filter" : {
# 	                "term" : { 
# 	                    "city" : "absecon"
# 	                }
# 	            }
# 	        }
# 	    }
# 	}

# q4 = {
#   "query" : {
#         "term" : { "city" : "absecon" }
#     }
# }


# matches = es.search(index="doctors-index")
# print len(matches['hits']['hits'])
# for doc in matches['hits']['hits']:
# 	print "iiiiiiiiiiiiiiiii",doc['_source']['full_name']
# # print matches
from elasticsearch import Elasticsearch
from elasticsearch_dsl import Search

client = Elasticsearch()

s = Search(using=client, index="doctors-index-test").query("match", city="absecon")

# s.aggs.bucket('per_tag', 'terms', field='tags') \
#     .metric('max_lines', 'max', field='lines')

response = s.execute()
print response,"<<<<<<<<<<<<<<"
for hit in response:
    print hit.full_name

# for tag in response.aggregations.per_tag.buckets:
#     print tag.key, tag.max_lines.value
