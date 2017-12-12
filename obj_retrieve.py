#!/usr/bin/env python

# Institution: Vanderbilt University
# Code created for the CS4287-5287 course
# Author: Aniruddha Gokhale
# Created: Fall 2016
#
# The purpose of this code is to access the huge file we have uploaded to the
# Horizon cloud container storage

# import the system basic files
import os
import sys
import time

# import the openstack swift project
from swiftclient import client

# http://docs.openstack.org/developer/python-swiftclient/swiftclient.html#module-swiftclient.client

#--------------------------------------------------------------------------
# The assumption below is that we have sourced our openrc file
#
# get our credentials from the environment variables. Note that we
# use the same names for the indexes as those needed by the parameters
# in the swift client connection request.
#
# Note, this is slightly different from the nova credentials but nonetheless
# both use authentication against the keystone URL
def get_swift_creds ():
    d = {}
    # These are obtained from our environment. Don't forget
    # to do "source cloudclass-rc.sh" or whatever is the name of your rc file
    #
    d['authurl'] = os.environ['OS_AUTH_URL']
    d['user'] = os.environ['OS_USERNAME']
    d['key'] = os.environ['OS_PASSWORD']
    d['tenant_name'] = os.environ['OS_TENANT_NAME']
    d['auth_version'] = '2.0'  # because we will be using the version 2 of the API
#    d['tenant_id'] = os.environ['OS_TENANT_ID']
#    d['region_name'] = os.environ['OS_REGION_NAME']
    return d

#----------------------------------------------------------------------------
# create a connection to the cloud using our credentials. We need this during
# container access
def create_connection (creds):
    # Now access the connection from which everything else is obtained.
    try:
        conn = client.Connection (**creds)
    except:
        print "Exception thrown: ", sys.exc_info()[0]
        raise

    return conn


#------------------------------------------------------------------------
# retrieve an object from the container
#
def retrieve_object (conn, cont_name, obj_name):
    try:
        respdict = {}
        obj_tuple = conn.get_object (cont_name, obj_name, response_dict=respdict)

        # this is for debugging only to get the metadata of the object
        print "Response dictionary = ", respdict
        
    except:
        print "Exception thrown: ", sys.exc_info()[0]
        raise

    return obj_tuple

#------------------------------------------------------------------------
# main function
#
def main ():

    print "Main function: get credentials"
    creds = get_swift_creds ()
    
    print "Main function: establish connection to horizon cloud"
    conn = create_connection (creds)
    
    print "Main function: retrieve object within the container storage"
    # In reality we need to retrieve this object but for demo purposes,
    # we are retrieving a relatively smaller text file
#    obj_tuple = retrieve_object (conn, "CloudClassSmartHome", "energy-sorted100M.csv")
    obj_tuple = retrieve_object (conn, "CloudClassSmartHome", "big.txt")

    print "Content length = ", obj_tuple[0]['content-length']

    # Note that the actual contents of the object can be retrieved in
    # the 2nd element of the tuple, in our case: obj_tuple[1]

    # Now you need to use the contents of the csv file to do the MapReduce
    # assignment found in obj_tuple[1]. The content-length as shown above
    # will tell you the total content. Since the CSV file is terminated
    # with a newline, you could use that as a delimiter to read the
    # required number of chunks for the M map tasks.
    
    print "Main function: success retrieving the object"
#------------------------------------------------------------------------
# invoke main
if __name__ == "__main__":
    sys.exit (main ())
    
