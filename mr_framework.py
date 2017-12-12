#!/usr/bin/python
#  
#
# Vanderbilt University Computer Science
# Author: Aniruddha Gokhale
# Course: CS4287-5287 Principles of Cloud Computing
# Created: Fall 2016
#
# Purpose: MapReduce Framework for Wordcount (not a generic framework)
#

# system and time
import os
import sys
import time
import re          # regular expression
import csv         # deal with CSV files
import operator    # used in itertools
import itertools   # nice iterators

from mr_thread import MR_Thread  # our Map Reduce threading class

from mininet.net import Mininet
from mininet.node import Node
from mininet.link import *
from mininet.log import  setLogLevel, info
from threading import Timer

import pickle

# ------------------------------------------------
def map_func (arg):
    """ Word count map function """
    print "map thread with name: ", arg['name']

    # Each map task saves its intermediate results in a file
    map_file = open (arg['name']+".csv", "w")
    
    # split the incoming chunk, which is a string. We want the
    # list to be only words and nothing else. So rather than the simple
    # split method of the string class, we use regexp's split

    # this is not working so I had to put things explicitly
    # split_arg = re.split ("(\s|(?![A-Za-z]))+", arg['data'])
    #
    # We allow apostrophe
    pattern = "(\s|~|`|\!|@|\#|\$|%|\^|&|\*|\(|\)|-|_|\+|=|\{|\}|\[|\]|\||\||:|;|\"|<|>|\,|\.|\?|\/)+"
    split_arg = re.split (pattern, arg['data'])

    # create a reg expression pattern against which we are going
    # to match against. We allow a word with apostrophe
    pattern = re.compile ("([A-Za-z]+)('[A-Za-z])?")
    
    # For every element in the split, if it belongs to a sensical
    # word, emit it as an intermediate key with its count
    for token in split_arg:
        # now check if it is a valid word
        if pattern.match (token):
            map_file.write (token + ", 1\n")

    # close the file
    map_file.close ()
    print "map thread with name: ", arg['name'], " exiting"
    
# ------------------------------------------------
def reduce_func (arg):
    """ Word count reduce function """
    print "reduce thread with name: ", arg['name']

    # Each reduce task saves its results in a file
    reduce_file = open (arg['name']+".csv", "w")
    
    # Note, each reduce job gets a list of lists. We need to sum up for each
    # internal list. The list of lists appears as the param arg['data']
    # Note that each second level list here is the list of entries for a given
    # unique key.
    for i in range (len (arg['data'])):
        # The i_th list represents a unique key and one or more entries
        list_per_word = arg['data'][i]

        reduce_file.write (list_per_word[0][0]+", ")
        result = 0
        # now let us sum up the results per word
        for j in range (len (list_per_word)):
            result += int (list_per_word[j][1])
        reduce_file.write (str(result) + "\n")

    reduce_file.close ()
    print "reduce thread with name: ", arg['name'], " exiting"

# ------------------------------------------------
# Main map reduce class        
class MR_Framework ():
    """ The map reduce orchestrator class """
    def __init__ (self, doc_name, m, r):
        self.doc_name = doc_name
        self.M = m            # num of map jobs
        self.R = r            # num of reduce jobs
        self.uniquekeys = []  # num of unique keys
        self.groups = []      # groups per unique key

        self.build_mininet()
    # ------------------------------------------------
    # function to build the topology
    def build_mininet(self):
        host_count = self.M if self.M > self.R else self.R
        self.net = Mininet()
        self.switch = self.net.addSwitch('s1')
        self.hosts = []
        self.master = self.net.addHost('m0')
        self.net.addLink(self.master, self.switch)
        # add hosts
        for i in range(host_count):
            host = self.net.addHost('h' + str(i))
            self.net.addLink(host, self.switch)
            self.hosts.append(host)

        self.net.start()


    # ------------------------------------------------
    # shuffle function. We are assured that there are no
    # stragglers because we use the barrier synchronization
    def shuffle_func (self):
        """ Word count shuffle function """
        print "\nShuffle phase"

        # Potentially we could have done all the reduction here itself
        # given we are in one process and all files are in the same
        # directory. But we will not do it that way to remain in the
        # spirit for MapReduce
        
        # for each csv file generated in the map phase
        # we sort the keys.
        for i in range (self.M):
            # open the CVS file created by map job
            csvfile = csv.reader (open ("Map"+str(i)+".csv", "r"), delimiter=",")
            
            # get the sorted list of entries from our csv file using
            # column 1 (0-based indexing used here) as the key to sort on
            # and we use traditional alphabetic order
            wordlist = sorted (csvfile, key=operator.itemgetter (0))

            # Now group all entries by uniquely identified words and
            # perform a combiner optimization (because it is addition)
            groups = []
            uniquekeys = []

            for k, g in itertools.groupby (wordlist, key=operator.itemgetter (0)):
                groups.append (list (g))
                uniquekeys.append (k)

            # Now create a temp file with combiner optimization
            shufflefile = open ("Shuffle"+str(i)+".csv", "w")

            for i in range (len (uniquekeys)):
                shufflefile.write (uniquekeys[i] + ", ")
                # note that for each unique word, we have a grouped
                # list, and each value is 1. So the length of the
                # sublist is the combiner optimization reduction
                # for that unique key. We save that info
                shufflefile.write (str (len (groups[i])) + "\n")

            shufflefile.close ()
            
        # for each csv file generated by the above local shuffling, we
        # combine all the results into one CVS file and redo the steps above
        temp_file = open ("temp.csv", "w")
        for i in range (self.M):
            shuffle_file = open ("Shuffle"+str(i)+".csv", "r")
            for line in shuffle_file:
                temp_file.write (line)

        temp_file.close ()

        # open the CVS file created by map job
        csvfile = csv.reader (open ("temp.csv", "r"), delimiter=",")
            
        # get the sorted list of entries from our csv file using
        # column 1 (0-based indexing used here) as the key to sort on
        # and we use traditional alphabetic order
        wordlist = sorted (csvfile, key=operator.itemgetter (0))

        for k, g in itertools.groupby (wordlist, key=operator.itemgetter (0)):
            self.groups.append (list (g))
            self.uniquekeys.append (k)

        print "Total unique keys = ", len (self.uniquekeys)

    # ------------------------------------------------
    # finalize function. We are assured that there are no
    # remaining reduce jobs because we use the barrier synchronization
    def finalize_func (self):
        """ Word count finalize function """
        print "Finalie phase: Aggregate all the results"

        # effectively, we go thru all the reduce results files and
        # get the results
        results = open ("results.csv", "w")
        for i in range (self.R):
            reduce_file = open ("Reduce"+str(i)+".csv", "r")
            data = reduce_file.read ()
            results.write (data)
            reduce_file.close ()
            
        results.close ()

        # cleanup. Delete all map, shuffle and reduce files
        os.remove ("temp.csv")
        for i in range (self.M):
            os.remove ("Map"+str(i)+".csv")
            os.remove ("Shuffle"+str(i)+".csv")
        for i in range (self.R):
            os.remove ("Reduce"+str(i)+".csv")

    # ------------------------------------------------
    # The method that solves the problem using the map reduce approach
    def solve (self):
        """Solve the problem using map reduce"""

        try:
            # initialize an array of threads
            # threads = []
            hosts = []

            start_time = time.time ()
            ########### Phase 1: Map ###################
            # find the file size and break it into (almost) equal
            # sized chunks
            # let's first open the file for reading
            doc = open (self.doc_name, 'r')

            doc_size = os.path.getsize (self.doc_name)
            chunk_size = int (round (doc_size/self.M))  # integer division
            print "doc size = ", doc_size, ", chunk size = ", chunk_size

            bytes_read = 0  # number of bytes read

            # Here we will create M number of threads to do the map operation
            #
            # Note that we are splitting the file along bytes so it is
            # very much possible that a valid word may get split into
            # nonsensical two words but we don't care here and will treat
            # these two split parts of a word as separate unique words
            print "\nMapReduce starting ", self.M, " map tasks"
            for i in range(self.M):
                # get the next chunk of data from the file
                if (i == self.M-1): # if this is the last chunk
                    chunk_size = doc_size - bytes_read

                chunk_content = doc.read (chunk_size)
                bytes_read += chunk_size

                # save file for access from mininet
                tmp_file = open("Map"+str(i)+"tmp.txt", "w")
                tmp_file.write(chunk_content)
                tmp_file.close()

                # create the thread and pass the chunk to it
                # thr = MR_Thread ("Map"+str(i), map_func, chunk_content)
                # threads.append (thr)
                # thr.start ()

                hosts.append(self.hosts[i])
                self.hosts[i].sendCmd('python /cc/mr_thread.py Map' + str(i) + ' map')

            # now wait for map threads to exit. This is the
            # barrier synchronization point where we wait for all map
            # tasks to finish
            print "\n\nMapReduce waiting for the Map tasks to terminate"
            for host in hosts:
                host.waitOutput()
            # for thr in threads:
            #     thr.join ()

            # clean up thread array
            del hosts[:]
            # del threads[:]
            end_time = time.time ()

            print "***** Map phase required: ", (end_time-start_time), " seconds"

            start_time = time.time ()
            ########### Phase 2: Shuffle ###################
            # this is not done in parallel for our case
            # In reality shuffle with do the necessary things
            # in parallel and move things around so that the
            # reduce job can fetch the right things from the
            # right place. We do not have any such elaborate
            # mechanism.
            self.shuffle_func ()
            
            end_time = time.time ()

            print "***** Shuffle phase required: ", (end_time-start_time), " seconds"

            start_time = time.time ()
            ########### Phase 3: Reduce ###################
            # Here we will create R number of threads to
            # do the reduce operation. Now we split our huge
            # list depending on the number of unique keys we
            # identified
            start_index = 0
            range_len = int (round (len (self.uniquekeys)/self.R))
            print "\n\nEach reduce task gets around ", range_len, " keys to handle"
            # print "MapReduce starting R reduce tasks"
            print "\nMapReduce starting ", self.R, " Reduce tasks"
            for i in range(self.R):
                # create the thread
                if (i == self.R-1):
                    # last entry
                    # thr = MR_Thread ("Reduce"+str(i), reduce_func, self.groups[start_index:])

                    # save file for access from mininet
                    tmp_file = open("Reduce" + str(i) + "tmp.txt", "w")
                    with tmp_file as f:
                        pickle.dump(self.groups[start_index:], f)
                    tmp_file.close()

                    # execute
                    hosts.append(self.hosts[i])
                    self.hosts[i].sendCmd('python /cc/mr_thread.py Reduce'+str(i)+' reduce')

                else:
                    # thr = MR_Thread ("Reduce"+str(i), reduce_func, self.groups[start_index:range_len])

                    # save file for access from mininet
                    tmp_file = open("Reduce" + str(i) + "tmp.txt", "w")
                    with tmp_file as f:
                        pickle.dump(self.groups[start_index:range_len], f)
                    tmp_file.close()

                    hosts.append(self.hosts[i])
                    self.hosts[i].sendCmd('python /cc/mr_thread.py Reduce'+str(i)+' reduce')



                start_index += range_len
                # threads.append (thr)
                # thr.start ()


            # now wait for threads to exit
            print ("MapReduce waiting for the Reduce tasks to terminate")
            for host in hosts:
                host.waitOutput()
            # for thr in threads:
            #     thr.join ()

            # clean up thread array
            del hosts[:]
            # del threads[:]

            end_time = time.time ()

            print "***** Reduce phase required: ", (end_time-start_time), " seconds"

            start_time = time.time ()
            ########### Phase 4: Finalize ###################
            # Here we will aggregate the results and print them
            self.finalize_func ()
            end_time = time.time ()

            print "***** Finalize phase required: ", (end_time-start_time), " seconds"
        except:
            print "Unexpected error:", sys.exc_info()[0]
            raise
