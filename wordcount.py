#!/usr/bin/python
#  
#
# Vanderbilt University Computer Science
# Author: Aniruddha Gokhale
# Course: CS4287-5287 Principles of Cloud Computing
# Created: Fall 2016
#
# Purpose: Simple MapReduce Wordcount
#

# system and time
import os
import sys
import time
import getopt      # cmd line arg parsing
import re          # regular expression

from mr_framework import MR_Framework # our wordcount MR framework

#------------------------------------------
# main function
def main ():
    """ Main program """
    # default number of map and reduce tasks
    m = 10
    r = 2

    # parse command line args
    try:
        opts, args = getopt.getopt (sys.argv[1:], 'hm:r:')
    except getopt.GetoptError:
        print 'mapreduce.py [h] [-m <num map>] [-r <num reduce>] file'
        sys.exit(2)
        
    for opt, arg in opts:
        if opt in ("-h"):
            # this is to print usage
            print 'mapreduce.py [h] [-m <num map>] [-r <num reduce>] file'
            sys.exit()
        elif opt in ("-m"):
            m = int (arg)
        elif opt in ("-r"):
            r = int (arg)

    # get the document name
    doc = sys.argv[len(sys.argv) - 1]
    
    # now invoke the mapreduce framework
    mrf = MR_Framework (doc, m, r)
    mrf.solve ()

#------------------------------------------
# invoke main
if __name__ == "__main__":
    sys.exit (main ())
    
