#!/usr/bin/env python
# graphite-import-sysstat - tool to import sysstat bulk data
#
# Author: Michael Gruener <michael.gruener@bedag.ch>
#
# Changelog:
#   2015-08-11 v0.01 - Initial release
#
# TODO:
#   - clean up code duplication in do_* functions
#   - find better way to implement time normalization
#     current reference point is hardcoded as now-5h
#   - generalize the power-management section, it is a special case right now
#   - make sadf options and graphite server parameters
#   - add parameter to add custom path prefix for metrics

import json
import subprocess
import sys
import datetime
import time
import pickle
import socket
import struct

def do_simplekv(section,path,timestamp,skip=''):
    metrics = []
    for key,value in section.items():
        if key == skip: continue
        fullpath = path + ".%s" % key
        metrics.append((fullpath,(timestamp,value)))
    return metrics

def do_cpuload(section,path,timestamp):
    metrics = []
    for cpu in section:
        cpuname = cpu['cpu']
        metrics += do_simplekv(cpu,path + ".%s" % cpuname,timestamp,'cpu')
    return metrics

def do_disk(section,path,timestamp):
    metrics = []
    for disk in section:
        device = disk['disk-device']
        metrics += do_simplekv(disk,path + ".%s" % device,timestamp,'disk-device')
    return metrics

def do_filesystems(section,path,timestamp):
    metrics = []
    for filesystem in section:
        fsname = filesystem['filesystem']
        metrics += do_simplekv(filesystem,path + ".%s" % fsname,timestamp,'filesystem')
    return metrics

def do_interrupts(section,path,timestamp):
    metrics = []
    for interrupt in section:
        fullpath = path + ".%s" % interrupt['intr']
        metrics.append((fullpath,(timestamp,interrupt['value'])))
    return metrics

def do_network(section,path,timestamp):
    metrics = []
    for subsectionname,subsection in section.items():
        if subsectionname in ["net-dev", "net-edev"]:
            for device in subsection:
                metrics += do_simplekv(device,path + ".%s.%s" % (subsectionname,device['iface']),timestamp,'iface')
        else:
            metrics += do_simplekv(subsection,path + ".%s" % subsectionname,timestamp)
    return metrics

def do_power_management(section,path,timestamp):
    metrics = []
    for subsectionname,subsection in section.items():
        # the usb-devices section does not contain performance data,
        # only a list of usb devices
        if subsectionname == "usb-devices": continue
        if subsectionname == "cpu-frequency": 
            for cpu in subsection:
                fullpath = path + ".cpu-frequency.%s" % cpu['number']
                metrics.append((fullpath,(timestamp,cpu['frequency'])))
        else:
            print "warning: ignored subsection %s while processing power-management data." % subsectionname
    return metrics

def do_serial(section,path,timestamp):
    metrics = []
    serial_id = 0
    for serial in section:
        metrics += do_simplekv(serial,path + ".%d" % serial_id,timestamp)
        serial_id += 1
    return metrics

if len(sys.argv) < 2:
    print "Usage: %s <sarfile>" % sys.argv[0]
    sys.exit(1)

sarfile = sys.argv[1]

sardata_raw = subprocess.check_output(["sadf","-j",sarfile,"--","-A"],shell=False)
sardata = json.loads(sardata_raw)

now = time.time() - 5 * 3600
timediff = 0

conn = socket.create_connection(("localhost",2004))
for host in sardata['sysstat']['hosts']:
    hostname = host['nodename']
    path = "sysstat.%s" % hostname
    for statistics in host['statistics']:
        # no sense in trying to import a metric that has no timestamp and there can be empty
        # records in sadf json data
        if 'timestamp' in statistics:
            datetime_text = "%s %s" % (statistics['timestamp']['date'],statistics['timestamp']['time'])
            datetime_format = '%Y-%m-%d %H:%M:%S'
            if statistics['timestamp']['utc'] == 1:
                datetime_text += " UTC"
                datetime_format += " %Z"
            timestamp = int(datetime.datetime.strptime(datetime_text,datetime_format).strftime("%s"))
            if timediff == 0: timediff = now - timestamp
            timestamp += timediff
            for sectionname,section in statistics.items():
                metrics = []
                if sectionname == timestamp: continue
                if sectionname == 'cpu-load-all': metrics += do_cpuload(section,path + ".cpu-load-all",timestamp)
                if sectionname == 'disk': metrics += do_disk(section,path + ".disks",timestamp)
                if sectionname == 'filesystems': metrics += do_filesystems(section,path + ".filesystems",timestamp)
                if sectionname == 'hugepages': metrics += do_simplekv(section,path + ".hugepages",timestamp)
                if sectionname == 'interrupts': metrics += do_interrupts(section,path + ".interrupts",timestamp)
                if sectionname == 'io': metrics += do_simplekv(section,path + ".io",timestamp)
                if sectionname == 'kernel': metrics += do_simplekv(section,path + ".kernel",timestamp)
                if sectionname == 'memory': metrics += do_simplekv(section,path + ".memory",timestamp)
                if sectionname == 'network': metrics += do_network(section,path + ".network",timestamp)
                if sectionname == 'paging': metrics += do_simplekv(section,path + ".paging",timestamp)
                if sectionname == 'power-management': metrics += do_power_management(section,path + ".power-management",timestamp)
                if sectionname == 'process-and-context-switch': metrics += do_simplekv(section,path + ".process-and-context-switch",timestamp)
                if sectionname == 'queue': metrics += do_simplekv(section,path + ".queue",timestamp)
                if sectionname == 'serial': metrics += do_serial(section,path + ".serial",timestamp)
                if sectionname == 'swap-pages': metrics += do_simplekv(section,path + ".swap-pages",timestamp)
                if metrics:
                    payload = pickle.dumps(metrics, protocol=2)
                    header = struct.pack("!L", len(payload))
                    message = header + payload
                    conn.sendall(header)
                    conn.sendall(payload)
conn.close()
