#!/usr/bin/python
#  
#
# Vanderbilt University Computer Science
# Author: Aniruddha Gokhale
# Course: CS4287-5287 Principles of Cloud Computing
# Created: Fall 2016
#
# Purpose: MapReduce Threading
#

# system and time
import os
import sys
import time
import re          # regular expression
import csv         # deal with CSV files
import operator    # used in itertools
import itertools   # nice iterators

# shared data structure and threading
import threading

import pickle

# ------------------------------------------------
def map_func(arg):
    """ Word count map function """
    print "map thread with name: ", arg['name']

    # Each map task saves its intermediate results in a file
    map_file = open('/cc/' + str(arg['name'])+".csv", "w")
    map2_file = open('/cc/booty.csv', "w")

    # split the incoming chunk, which is a string. We want the
    # list to be only words and nothing else. So rather than the simple
    # split method of the string class, we use regexp's split

    # this is not working so I had to put things explicitly
    # split_arg = re.split ("(\s|(?![A-Za-z]))+", arg['data'])
    #
    # We allow apostrophe
    pattern = "(\s|~|`|\!|@|\#|\$|%|\^|&|\*|\(|\)|-|_|\+|=|\{|\}|\[|\]|\||\||:|;|\"|<|>|\,|\.|\?|\/)+"
    split_arg = re.split(pattern, arg['data'])

    # create a reg expression pattern against which we are going
    # to match against. We allow a word with apostrophe
    pattern = re.compile("([A-Za-z]+)('[A-Za-z])?")

    # For every element in the split, if it belongs to a sensical
    # word, emit it as an intermediate key with its count
    for token in split_arg:
        # now check if it is a valid word
        if pattern.match(token):
            map_file.write(token + ", 1\n")

    # close the file
    map_file.close()
    print "map thread with name: ", arg['name'], " exiting"


# ------------------------------------------------
def reduce_func(arg):
    """ Word count reduce function """
    print "reduce thread with name: ", arg['name']

    # Each reduce task saves its results in a file
    reduce_file = open ('/cc/' + str(arg['name'])+".csv", "w")

    # Note, each reduce job gets a list of lists. We need to sum up for each
    # internal list. The list of lists appears as the param arg['data']
    # Note that each second level list here is the list of entries for a given
    # unique key.
    for i in range(len(arg['data'])):
        # The i_th list represents a unique key and one or more entries
        list_per_word = arg['data'][i]

        reduce_file.write(list_per_word[0][0] + ", ")
        result = 0
        # now let us sum up the results per word
        for j in range(len(list_per_word)):
            result += int(list_per_word[j][1])
        reduce_file.write(str(result) + "\n")

    reduce_file.close()
    print "reduce thread with name: ", arg['name'], " exiting"


# subclass from the threading.Thread
class MR_Thread (threading.Thread):


    # constructor
    def __init__ (self, name, func):
        threading.Thread.__init__(self)

        self.arg = {'name' : name,
                    'data' : None}
        testF = open('/cc/testf.txt', 'w')
        testF.write(str(self.arg['name']) + ' : \n')
        testF.close()

        tmp_file = open(name + "tmp.txt", "r")

        if func == "map":
            self.func = map_func
            self.arg.data = tmp_file.read()
        elif func == "reduce":
            self.func = reduce_func
            with tmp_file as f:
                self.arg.data = pickle.load(f)

        tmp_file.close()
        os.remove('/cc/' + name + "tmp.txt")


    # override the run method which is invoked by start
    def run (self):
        print "Starting " + self.name
        self.func (self.arg)
        print "Exiting " + self.name


if __name__ == "__main__":
    job = MR_Thread(sys.argv[1], sys.argv[2])
    sys.exit(job.run())


