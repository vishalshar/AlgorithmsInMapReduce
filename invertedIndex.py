#Create an Inverted index. Given a set of documents, an inverted index is a dictionary where each word is associated with a list of the document identifiers in which that word appears.


import MapReduce
import sys

"""
Inverted Index: Simple Python MapReduce Framework
"""


# Part 1
mr = MapReduce.MapReduce()

# Part 2
def mapper(record):
    # key: document identifier
    # value: document contents
    key = record[0]
    value = record[1]
    words = value.split()
    for w in words:
      mr.emit_intermediate(w, key)

# Part 3
def reducer(key, list_of_values):
    # key: word
    # value: list of occurrence counts
    total = {}
    for v in list_of_values:
        if not total.has_key(v):
            total[v] = v
    mr.emit((key,total.keys()))

# Part 4
if __name__ == '__main__':
  inputdata = open(sys.argv[1])
  mr.execute(inputdata, mapper, reducer)
