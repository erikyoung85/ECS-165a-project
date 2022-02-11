from lstore.db import Database
from lstore.query import Query
from time import process_time
from random import choice, randrange

db = Database()
grades_table = db.create_table('Grades', 6, 0)
query = Query(grades_table)
keys = []

n = 10000

#Inserting and selecting a specific value
print("Inserting and Selecting [123456, 1, 2, 3, 4, 5]:")
query.insert(123456, 1, 2, 3, 4, 5)
record = query.select(123456, 0, [1, 1, 1, 1, 1, 1])
print(record[0].columns)

#Select only primary key
print("We can also select only the primary key:")
record = query.select(123456, 0, [1, 0, 0, 0, 0, 0])
print(record[0].columns)

print("You cannot add a duplicate primary key:")    
#Show that primary key cannot be duplicated
query.insert(10, 9, 8, 7, 6, 5)
query.insert(10, 1, 2, 3, 4, 5)


record = query.select(10, 0, [1, 1, 1, 1, 1, 1])
print(record[0].columns)

print("Now, we update [123456, 1, 2, 3, 4, 5] to [6,7,8,9,10] and select it")
#Updating and selecting a specific entry
query.update(123456, *[6, 7, 8, 9, 10, 11])
record = query.select(123456, 0, [1, 1, 1, 1, 1, 1])
print(record[0].columns)

#Delete a specific record
print("Deleting and trying to select a record. It is not there")
query.delete(123456)
record = query.select(123456, 0, [1, 1, 1, 1, 1, 1])
print(record)

print("And now, demonstrating the 5 functions using " + str(n) + " records")
#Demonstrate insert
insert_time_0 = process_time()
for i in range(0, n):
    query.insert(i, 10*i, 3+i, i**2, 1000 + i, 2*i)
    keys.append(i)
insert_time_1 = process_time()

print("Inserting " + str(n) + " records took:  \t\t\t", insert_time_1 - insert_time_0)

update_cols = [
    [None, None, None, None, None, None],
    [None, randrange(0, 100), None, None, None, None],
    [None, None, randrange(0, 100), None, None, None],
    [None, None, None, randrange(0, 100), None, None],
    [None, None, None, None, randrange(0, 100), None],
    [None, None, None, None, None, randrange(0, 100)]
]

#Demonstrate update
update_time_0 = process_time()
for i in range(0, n):
    query.update(choice(keys), *(choice(update_cols)))
update_time_1 = process_time()
print("Updating " + str(n) + " records took:  \t\t\t", update_time_1 - update_time_0)

#Demonstrate select
select_time_0 = process_time()
for i in range(0, n):
    query.select(choice(keys),0 , [1, 1, 1, 1, 1, 1])
select_time_1 = process_time()
print("Selecting " + str(n) + " records took:  \t\t\t", select_time_1 - select_time_0)

#Demonstrate aggregate
agg_time_0 = process_time()
for i in range(0, n, 100):
    start_value = i
    end_value = start_value + 100
    result = query.sum(start_value, end_value - 1, randrange(0, 6))
agg_time_1 = process_time()
print("Aggregate " + str(n) + " of 100 record batch took:\t\t", agg_time_1 - agg_time_0)

#Demonstrate delete
delete_time_0 = process_time()
for i in range(0, n):
    query.delete(i)
delete_time_1 = process_time()
print("Deleting " + str(n) + " records took:  \t\t\t", delete_time_1 - delete_time_0)

print("TOTAL TIME: \t\t\t\t\t", (insert_time_1 - insert_time_0) + (update_time_1 - update_time_0) + (select_time_1 - select_time_0) + (agg_time_1 - agg_time_0) + (delete_time_1 - delete_time_0))