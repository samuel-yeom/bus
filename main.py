import xml.etree.ElementTree as ET
import requests
from random import random
import time
import os
from warnings import warn

url = 'http://realtime.portauthority.org/bustime/map/getStopPredictions.jsp'
params = {'stop': '7117', 'route': '61A,61B,61C,61D', 'key': repr(random())}

'''all_buses is a dictionary of buses that we are currently tracking.
Keys are vehicle numbers, and values are tuples (route, eta, last_seen), where
route is the route number, eta is estimated time to arrival, and last_seen is
a Unix time of when the bus was last seen.'''
all_buses = {}

'''Gets live bus data from Pittsburgh Port Authority website. If successful,
returns a tuple (text, timestamp), where text is the live bus data as an XML
string, and timestamp is the current Unix time. If the web connection fails,
returns (None, timestamp).'''
def get_xml_data():
    params['key'] = repr(random()) #use new random number to avoid caching
    try:
        r = requests.get(url, params)
        r.raise_for_status()
    except requests.RequestException: #if there was an error getting the data
        return (None, int(time.time()))
    
    return (r.text, int(time.time()))

'''Convert the XML into a dictionary containing the bus data. The key is the
vehicle number, and the value is the tuple (route, eta, timestamp), where route
is the route number, eta is the estimated time to arrival, timestamp represents
the Unix time at which the data was retrieved from the Port Authority website.
If the parser fails, returns None.'''
def parse_xml_bus_data(text, timestamp):
    buses = {}
    
    try:
        root = ET.fromstring(text)
    except TypeError, ET.ParseError: #text is None or parser fails
        return None
    
    for bus_node in root:
        if bus_node.tag != 'pre': #if this node does not describe a bus
            continue
        
        #Collect relevant info about this bus
        bus_info = {}
        relevant_tags = ['pt', 'v', 'scheduled', 'rn']
        for node in bus_node:
            if node.tag in relevant_tags:
                bus_info[node.tag] = node.text
        
        assert all([tag in bus_info for tag in relevant_tags])
        
        #Add this bus to the dictionary of buses
        if bus_info['scheduled'] == 'false': #if bus data comes from GPS
            vnum = bus_info['v'] #vehicle number
            route = bus_info['rn'] #route number
            eta = bus_info['pt'] #estimated time to arrival
            buses[vnum] = (route, eta, timestamp)
    
    return buses

'''Gets live bus data from Pittsburgh Port Authority website and updates
all_buses, a global dictionary of buses that we are currently tracking. Then,
If a bus in the dictionary has not been updated for 5 minutes (usually happens
because the bus has arrived and left the station), writes information about the
bus to the currently open file f.
'''
def update_buses(f):
    text, timestamp = get_xml_data()
    recent_buses = parse_xml_bus_data(text, timestamp)
    time_str = time.strftime('%H:%M:%S', time.localtime(timestamp))
    
    if recent_buses is None: #if web connection or parser failed
        if text is None:
            warn('Web connection failed at {}'.format(time_str))
        else:
            warn('XML Parser failed at {}'.format(time_str))
    
    else:
        for bus in recent_buses:
            all_buses[bus] = recent_buses[bus]
    
    #Deal with buses that have not been updated for a while
    for bus in all_buses.keys():
        vnum = bus #vehicle number
        route, eta, last_seen = all_buses[bus] #last_seen is a Unix time
        if timestamp - last_seen > 300: #if last seen more than 5 minutes ago
            last_seen_str = time.strftime('%H:%M:%S', time.localtime(last_seen))
            line = '{},{},{},{}\n'.format(last_seen_str, route, vnum, eta)
            
            #Write to file
            f.write(line)
            f.flush()
            os.fsync(f.fileno())
            
            #Stop tracking the bus
            del all_buses[bus]

'''Returns the current date in local time in yyyymmdd format. A day is
considered to begin at 3am local time.'''
def get_date():
    timestamp = int(time.time())
    timestamp -= 10800 #subtract 3 hours (a day begins at 3am)
    return time.strftime('%Y%m%d', time.localtime(timestamp))

if __name__ == '__main__':
    while True:
        filedate = get_date()
        filepath = 'data/buses-{}.csv'.format(filedate)
        
        with open(filepath, 'a') as f:
            while filedate == get_date():
                update_buses(f)
                time.sleep(10)