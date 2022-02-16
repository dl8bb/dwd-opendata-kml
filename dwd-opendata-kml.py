#!/usr/bin/env python3
# -*- coding: utf-8 -*-
#
# parse dwd opendata weather forecast
#
# Dirk Clemens, git@adcore.de 
#
# https://opendata.dwd.de/weather/local_forecasts/mos/MOSMIX_L/single_stations/E970/kml/MOSMIX_L_LATEST_E970.kmz
#
############################################################
# DWD Wetter
# DD      Windrichtung            146
# FF      Windgeschwindigkeit     26
# FX1     Maximale Windböe innerhalb der letzten Stunde      44
# Neff    Effektive Wolkendecke   66
# PPPP    Luftdruck               1014.2
# RR6c    Gesamtniederschlag während der letzten 6 Stunden, konsitent mit dem signifikanten Wetter       0.00
# TTT     Temperatur 2m über der Oberfläche     -0.30
# time    01:00
# ww      Signifikantes Wetter    2
# wwd     Wahrscheinlichkeit: Auftreten von Schichtniederschlägen innerhalb der letzten Stunden      Bewölkung unverändert
# SunD	  Sonnenscheindauer Vortag insgesamt	(s)
# RSunD	  Relative Sonnenscheindauer innerhalb der letzten 24 Stunden (%)
# Tx	  Maximale Temperatur - innerhalb der letzten 12 Stunden
# Tn 	  Mindesttemperatur - innerhalb der letzten 12 Stunden
############################################################
#

import zipfile
from lxml import etree
import re
import dateutil.parser
from io import BytesIO
from urllib.request import urlopen

import json
import time
import datetime
import calendar
from os.path import expanduser
from influxdb import InfluxDBClient

region        = 'E970'     # Dransfeld-Ossenfeld

dbHost        = '192.168.188.47'
dbPort        = 8086
dbUser        = 'admin'
dbPasswd      = ''
dbName        = 'enverbridge'
dbMeasurement = 'dwdweather'
dbToken       = ''
influxTag     = 'mosmix'
influxTopic   = 'weather/dwd/mosmix'

#      Länge * Breite * Anzahl
area = 1.675 * 0.992 * 4.0
# Wirkungsgrad: Modul  * Wechselrichter * senkrechte Aufstellung
efficiency = 0.1926 * 0.956 * 0.7

# Globalstrahlung * 0,278 (Umrechnung in Watt/m2) * Modulfläche * Wirkungsgrad
def rad1hToWatt(rad1h):
    return float(rad1h * 0.2777777777777778 * area * efficiency)

datePattern = "%Y-%m-%dT%H:%M:%S.%f"

def readPasswdFromFile(passwdFileName):
    if passwdFileName.startswith('~'):
        passwdFileName = expanduser(passwdFileName)
    pfile = open(passwdFileName, 'r')
    pw = pfile.readline()
    pfile.close()

    return str(pw.rstrip("\n"))

def toJson(dateStr, timeStamp, rad1h):
#    print("toJson(%s, %s, %s)" % (dateStr, timeStamp, rad1h))
    result = {
        "Date":          dateStr,
        "Timestamp":     timeStamp,
        "Rad1h":         float(rad1h),
        "ExpectedPower": round(rad1hToWatt(float(rad1h)), 3),
        "Area":          round(area, 3),
        "Efficiency":    round(efficiency, 3)
    }

    return result

def updateInflux(region, timeStamp, data):
#    print("updateInflux(%s %s %s)" % (region, timeStamp, json.dumps(data)))

    global dbPasswd
    dbPasswd = readPasswdFromFile("~/.db_passwd")

    client = InfluxDBClient(dbHost, dbPort, dbUser, dbPasswd, dbName)

    points = [
        {
            "time": timeStamp,
            "measurement": dbMeasurement,
            "tags": {
                "host":   "tuya",
                "topic":  influxTopic,
                "region": region
            },
            "fields": data
        }
    ]

#    print("updateInflux: ", json.dumps(points))

    # precision is in seconds
    time_precision = 's'

    # write the datapoint
    client.write_points(
        points = points,
        time_precision = time_precision
    )

def numeric(s):
    try:
        if '-' in s:
            return 0
        else:
            return int(s)
        pass
    except ValueError:
        return round(float(s)*1.0, 1)

def getElementValueAsList(tree, element):
    for df in tree.xpath('////*[name()="dwd:Forecast" and @*[name()="dwd:elementName" and .="%s"]]' % element):
        # strip unnecessary whitespaces
        elements = re.sub(r'\s+', r';', str(df.getchildren()[0].text).lstrip(' '))
        # print (elements)
        lst = elements.split(";")
        # print (len(lst))
        for index, item in enumerate(lst):  # convert from string
            lst[index] = numeric(lst[index])
        return lst

def analyse(tree):
    # That first part in "double" tag name stands for namespace.
    # You can ignore namespace while selecting element in lxml
    # as //*[name()="Placemark"]/*[name()="ExtendedData"]...
    # The same for attributes: //*[name()="Forecast" and @*[name()="elementName"]]


    #<kml:description>DUESSELDORF</kml:description>
    for df in tree.xpath('////*[name()="kml:description"]'):
        print (df.text)

    #<dwd:IssueTime>2018-12-18T15:00:00.000Z</dwd:IssueTime>
    for df in tree.xpath('////*[name()="dwd:IssueTime"]'):
        print (dateutil.parser.parse(df.text).__format__(datePattern))
        # print (dateutil.parser.parse(df.text).__format__("%d.%m.%YT%H:%M:%S.%fZ"))

    #print ('\n')

    ele_TimeStamp = []
    for df in tree.xpath('//*[name()="dwd:ForecastTimeSteps"]'):
        timeslots = df.getchildren()
        for timeslot in timeslots:
            # print ('timeslot=' + timeslot.text)
            # tm = dateutil.parser.parse(timeslot.text).__format__("%d.%m.%YT%H:%M:%S.%fZ")
            tm = dateutil.parser.parse(timeslot.text).__format__(datePattern)
            #tm = timeslot.text
            ele_TimeStamp.append(tm)
#    print("Time (", len(ele_TimeStamp), ") ", ele_TimeStamp)

    ele_Rad1h = getElementValueAsList(tree, 'Rad1h')
#    print("Rad1h (", len(ele_Rad1h), ") ", ele_Rad1h)

#    ele_PPPP = getElementValueAsList(tree, 'PPPP')  # =x/100
#    for index, item in enumerate(ele_PPPP):
#        ele_PPPP[index] = float(ele_PPPP[index]) / 100.0
#    print("PPPP (", len(ele_PPPP), ") ", ele_PPPP)
#
#    ele_FX1 = getElementValueAsList(tree, 'FX1')
#    print("FX1 (", len(ele_FX1), ") ", ele_FX1)
#
#    ele_ww = getElementValueAsList(tree, 'ww')
#    print("ww  (", len(ele_ww), ") ", ele_ww)
#
#    ele_SunD = getElementValueAsList(tree, 'SunD')  # =round(x)
#    for index, item in enumerate(ele_SunD):
#        ele_SunD[index] = round(float(ele_SunD[index]), 2)
#    print("SunD (", len(ele_SunD), ") ", ele_SunD)
#
#    ele_TX = getElementValueAsList(tree, 'TX')  # =x-273.15
#    for index, item in enumerate(ele_TX):
#        if (int(ele_TX[index]) > 99):
#            ele_TX[index] = round(float(ele_TX[index]) - 273.15, 2)
#    print("TX  (", len(ele_TX), ") ", ele_TX)
#
#    ele_Tn = getElementValueAsList(tree, 'TN')  # =x-273.15
#    for index, item in enumerate(ele_Tn):
#        if (int(ele_Tn[index]) > 99):
#            ele_Tn[index] = round(float(ele_Tn[index]) - 273.15, 2)
#    print("Tn  (", len(ele_Tn), ") ", ele_Tn)
#
#    ele_Neff = getElementValueAsList(tree, 'Neff')  # =x*8/100
#    for index, item in enumerate(ele_Neff):
#        ele_Neff[index] = float(ele_Neff[index]) * 8 / 100
#    print("Neff (", len(ele_Neff), ") ", ele_Neff)
#
#    ele_R101 = getElementValueAsList(tree, 'R101')
#    print("R101 (", len(ele_R101), ") ", ele_R101)

    for index, item in enumerate(ele_TimeStamp):
        dt = datetime.datetime.strptime(item, datePattern).timetuple()
        #dtLocal = calendar.timegm(dt)
        dtLocal = int(time.mktime(dt))
        rad1h = ele_Rad1h[index]
        print("index: ", index, ", dateStr: ", item, ", timestamp: ", dtLocal, ", Rad1h: ", rad1h)
        updateInflux(region, dtLocal, toJson(item, dtLocal, rad1h))

def go():
    url = 'https://opendata.dwd.de/weather/local_forecasts/mos/MOSMIX_L/single_stations/' + region + '/kml/MOSMIX_L_LATEST_' + region + '.kmz'
    #url = 'https://opendata.dwd.de/weather/local_forecasts/mos/MOSMIX_L/single_stations/E970/kml/MOSMIX_L_2021110821_E970.kmz'
    #url = 'http://192.168.1.134/MOSMIX/MOSMIX_L_2021110821_E970.kmz'

    kmz = zipfile.ZipFile(BytesIO(urlopen(url).read()), 'r')
    #kmz = zipfile.ZipFile('MOSMIX_L_LATEST_E970.kmz', 'r')

    kml_filename = kmz.namelist()[0]
    #print (kml_filename)

    tree = etree.parse(kmz.open(kml_filename, "r"))
    #print (tree)

    analyse(tree)

    kmz.close()


if __name__ == '__main__':
    #print ('\n')
    go()
