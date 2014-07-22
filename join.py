#Implement a relational join as a MapReduce query
import MapReduce
import sys

"""
Relational Join: Implement a relational join as a MapReduce query

Consider the following query:
SELECT * 
FROM Orders, LineItem 
WHERE Order.order_id = LineItem.order_id

+ MapReduce query should produce the same result as this SQL query executed against an appropriate database.
+ Consider the two input tables, Order and LineItem, as one big concatenated bag of records that will be processed by the map function record by record.
"""


# Part 1
mr = MapReduce.MapReduce()


# Part 2
def mapper(record):
    # key : key Id of contents
    key = record[1]
    mr.emit_intermediate(key,record)


# Part 3
def reducer(key, list_of_values):
    # key: identifier Id
    # value: list of tuples having same Id
    for i in list_of_values:
        if i[0].lower() == 'order':
            for j in list_of_values:
                if j[0].lower() == 'line_item':
                    join = i + j
                    print join
                    mr.emit(join)


# Part 4
if __name__ == '__main__':
  inputdata = open(sys.argv[1])
  mr.execute(inputdata, mapper, reducer)
