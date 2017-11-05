import names
from random import randint
import numpy as np
import pymongo
from pymongo import MongoClient
import matplotlib.pyplot as plt


client = MongoClient('localhost', 27017)
db = client.test_db

# Inserting random data
office_titles = ['developer','business manager','staff','senior deveolper','marketing analyst',
                 'business analyst','director','architect','junior developer']
countries = ['USA','UK','PAK','IND','CHN','RUS','GER','JPN','FRA','AUS']

#
# # Record insertion
# for record in range(1000):
#     title = np.random.choice(office_titles, 1, p=[0.2, 0.05, 0.2, 0.1, 0.1, 0.1, 0.05, 0.05, 0.15 ])[0]
#     first_nme = names.get_first_name()
#     last_name = names.get_last_name()
#     age = randint(20, 40)
#     country = np.random.choice(countries, 1, p=[0.2, 0.05, 0.2, 0.1, 0.1, 0.1, 0.05, 0.05, 0.1, 0.05 ])[0]
#     salary_range = [randint(35000,50000), randint(70000,85000), randint(25000,45000),randint(55000,75000),
#               randint(35000,75000),randint(30000,60000),
#               randint(90000,120000),randint(45000,85000),randint(25000,35000)]
#     salary = salary_range[office_titles.index(title)]
#     db.employees.insert({'last_name': last_name, 'age': age, 'title': title,
#                          'first_name': first_nme, 'country':country, 'salary':salary})


# Count the number of records in a collection
count_collection = db.employees.count()
print(count_collection)


# min, max record of a field
min_record = db.employees.find().sort([('age',1)]).limit(1) # minimum age record in the collection
min_5_records = db.employees.find().sort([('age',1)]).limit(5) # 5 minimum age record in the collection

max_record = db.employees.find().sort([('age',-1)]).limit(1) # minimum age record in the collection
max_5_records = db.employees.find().sort([('age',-1)]).limit(5) # 5 minimum age record in the collection


# list of Distinct document values in a specific field
distinct_age = db.employees.distinct('age')
distinct_title = db.employees.distinct('title')
distinct_country = db.employees.distinct('country')


# Aggregate functions min, max, avg with group
minimum = db.employees.aggregate([{'$group': {'_id': '$title', 'minQuantity':{'$min':'$age'}}}])
maximum = db.employees.aggregate([{'$group': {'_id': '$title', 'maxQuantity':{'$max':'$age'}}}])
average = db.employees.aggregate([{'$group': {'_id': '$title', 'avgQuantity':{'$avg':'$age'}}}])

# count group by
titles = db.employees.aggregate([{'$group': {'_id': '$title', 'count':{'$sum':1}}}])
emp_country = db.employees.aggregate([{'$group': {'_id': '$country', 'count':{'$sum':1}}}])

# for i in emp_country:
#     print(i)

print('******************')
# SELECT * FROM EMPLOYEES WHERE COUNTRY=PAK GROUP BY TITLE
query_1 = db.employees.aggregate([
    {'$match':{'country':'PAK'}},
    {'$group': {'_id':'$title', 'count':{'$sum':1}}}
])

# SELECT * FROM EMPLOYEES WHERE COUNTRY=PAK AND AGE > 30 GROUP BY TITLE
query_2 = db.employees.aggregate([
    {'$match':{'$and':[{'country':'PAK'}, {'age':{'$gt':30}}]}},
    {'$group': {'_id':'$title', 'count':{'$sum':1}}}
])

# print(query_1)
# for i in query_1:
#     print(i)



# plot pie chart
data = []
for i in query_1:
    # print(i)
    data.append([i['_id'], i['count']])
data = list(zip(*data)) # reverse dimensions of a list
print(data)
company_titles, count_titles = data[0], data[1]
# Plot
plt.pie(count_titles, labels=company_titles)
plt.show()
