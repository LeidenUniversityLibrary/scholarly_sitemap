import re
import urllib
import urllib2
import os, os.path
import csv
from urllib2 import *
import json
import re
import gzip
import contextlib
import getopt
import sys
from datetime import datetime
from datetime import date
import xml.etree.ElementTree as ET

print "{0} -- Starting sitemap update".format(str(datetime.now()))

#Processes JSON response from SOLR into a usable line for a csv file
def processJsonResponse(fdocument, typestr):
	if typestr in fdocument:
		#Can return JSON list or a string
		if isinstance(fdocument[typestr], list):
			#Encode as UTF8 to allow string manipulation, then replace joins with a single comma, then replace any instances of semicolon with a colon
			return re.sub(';', ':', str(','.join([x.encode('UTF8') for x in document[typestr]])))
		else:
			return fdocument[typestr]
	else:
		return ''
	
solr_url = 'http://scharrelaar-p4.leidenuniv.nl:8081/solr/collection1/select?q='
work_dir = "/var/www/html/drupal7/sites/scholarlypublications.universiteitleiden.nl/files/xmlsitemap/NXhscRe0440PFpI5dSznEVgmauL25KojD7u4e9aZwOM/"
export_dir = "/home/ubbeheer/sitemap_scholarly/"
handle_file = work_dir + "updateset.csv"

#gets the number of xml files already made, -1 for the index file.
sitemaptotals = len([f for f in os.listdir(work_dir) if os.path.isfile(os.path.join(work_dir, f)) and f.endswith('.xml')]) -1

if sitemaptotals == 0:
	#Iemand heeft aan de sitemap module gezeten in Islandora zelf. Maak een compleet nieuwe sitemap aan.
	with open (work_dir + 'index.xml', "w") as indexfile:
		indexfile.write("<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n")
		indexfile.write("<?xml-stylesheet type=7\"text/xsl\" href=\"//scholarlypublications.universiteitleiden.nl/sitemap.xsl\"?>\n")
		indexfile.write("<sitemapindex xmlns=\"http://www.sitemaps.org/schemas/sitemap/0.9\">\n")
		indexfile.write("<sitemap><loc>http://scholarlypublications.universiteitleiden.nl/sitemap.xml?page=1</loc><lastmod>1900-01-01T00:00Z</lastmod></sitemap>\n")
		indexfile.write("</sitemapindex>")
		sitemaptotals = 1
	print("NIEUWE SITEMAP AANGEMAAKT!")

ET._namespace_map["http://www.sitemaps.org/schemas/sitemap/0.9"] = ''
mytree = ET.parse(work_dir + "index.xml")
myroot = mytree.getroot()
for index, urlset_xml in enumerate(myroot.findall('{http://www.sitemaps.org/schemas/sitemap/0.9}sitemap')):
	date_xml = urlset_xml.find('{http://www.sitemaps.org/schemas/sitemap/0.9}lastmod')
	last_up_date = datetime.strptime(date_xml.text, "%Y-%m-%dT%H:%MZ")
	
with open(handle_file, "w") as b:
	#Open connection and collect data
	query = 'ancestors_ms%3A%22collection%3Ascholarly%22+AND+RELS_EXT_hasModel_uri_ms%3A%22info%3Afedora%2Fislandora%3AcompoundCModel%22+AND+fgs_state_s%3A%22Active%22&fq=fedora_datastream_version_RELS-EXT_CREATED_ms%3A%5B' + last_up_date.strftime("%Y-%m-%dT%H:%M:%S.00Z") + '+TO+NOW%5D&rows=1000000&fl=mods_identifier_hdl_ms+fedora_datastream_version_MODS_CREATED_ms&wt=json&indent=true'
	try:
		connection = urlopen('{0}{1}'.format(solr_url, query))
	except urllib2.URLError, e:
		print '{0} -- Could not connect to {1} with error code "{2}".'.format(str(datetime.now()), solr_url + query, str(e))
		sys.exit(2)
	response = json.load(connection)

	#Parse through JSON response and write to file
	for document in response['response']['docs']:
		handleJsonvar = processJsonResponse(document, 'mods_identifier_hdl_ms')
		changeJsonvar = processJsonResponse(document, 'fedora_datastream_version_MODS_CREATED_ms')
		
		writeLine = '{0},{1},\n'.format(handleJsonvar,changeJsonvar)
		b.write(writeLine)

ids_size = os.path.getsize(handle_file)
update_set = dict()
insert_set = dict()
xlmns = {'x': 'http://www.sitemaps.org/schemas/sitemap/0.9'}

with open(handle_file, "r") as a:
	reader = csv.reader(a, delimiter=',')
	for index,row in enumerate(reader):
		handlevar = re.sub('https://hdl.handle.net/1887', 'http://scholarlypublications.universiteitleiden.nl/handle/1887', row[0])
		row_count = sum(1 for rows in row)
		startdate = datetime.strptime(row[1][:10], "%Y-%m-%d")
		if row_count > 3:
			lastddate = datetime.strptime(row[row_count-3][:10], "%Y-%m-%d")
		else:
			lastddate = startdate
		
		enddate = datetime.strptime(row[row_count-2][:10], "%Y-%m-%d")
		delta = enddate - lastddate
		
		lastmodvar = row[row_count-2][:16] + "Z"
		if delta.days / 7 <= 1:
			freqvar = "daily"
		elif delta.days / 7 <= 28:
			freqvar = "weekly"
		elif delta.days / 7 <= 364:
			freqvar = "monthly"
		else:
			freqvar = "yearly"
		
		if startdate > last_up_date:
			insert_set[handlevar] = (handlevar, lastmodvar, freqvar)
		else:
			update_set[handlevar] = (handlevar, lastmodvar, freqvar)
		
for x in (range(sitemaptotals)):
	
	print str(x + 1)
	
	ET._namespace_map["http://www.sitemaps.org/schemas/sitemap/0.9"] = ''
	mytree = ET.parse(work_dir + str(x+1) + ".xml")
	myroot = mytree.getroot()
	
	with open(work_dir + str(x+1) + ".xml", "w") as xmlfile:
		xmlfile.write("<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n")
		xmlfile.write("<?xml-stylesheet type=\"text/xsl\" href=\"//scholarlypublications.universiteitleiden.nl/sitemap.xsl\"?>\n")
		xmlfile.write("<urlset xmlns=\"http://www.sitemaps.org/schemas/sitemap/0.9\">\n")
	
		for index, urlset_xml in enumerate(myroot.findall('{http://www.sitemaps.org/schemas/sitemap/0.9}url')):
			loc_xml = urlset_xml.find('{http://www.sitemaps.org/schemas/sitemap/0.9}loc')
			date_xml = urlset_xml.find('{http://www.sitemaps.org/schemas/sitemap/0.9}lastmod')
			changefreq_xml = urlset_xml.find('{http://www.sitemaps.org/schemas/sitemap/0.9}changefreq')
			maxLength = index
			
			if loc_xml.text in update_set:
				datevar = update_set[loc_xml.text][1]
				freqvar = update_set[loc_xml.text][2]
			else:
				if date_xml != None:
					datevar = date_xml.text
					freqvar = changefreq_xml.text
			
			if loc_xml.text == "http://scholarlypublications.universiteitleiden.nl/":
				writeline = "<url><loc>" + loc_xml.text + "</loc><changefreq>daily</changefreq><priority>1.0</priority></url>\n"
			else:
				writeline = "<url><loc>" + loc_xml.text + "</loc><lastmod>" + datevar + "</lastmod><changefreq>" + freqvar + "</changefreq></url>\n"
			xmlfile.write(writeline)

		xmlfile.write("</urlset>\n")

#Check of er geen nieuwe inserts zijn sinds de vorige keer. Zo niet, doe +1 op sitemaptotals die anders in de volgende for loop toegevoegd zou worden.
#Dit zit achter een if else statement, omdat het mogelijk is om in de volgende for loop meerdere extra files aan te maken.
if len(insert_set) > 0:
	os.system('sed -i "$ d" {0}'.format(work_dir + str(sitemaptotals) + ".xml"))
else:
	sitemaptotals = sitemaptotals + 1

for index, y in enumerate(insert_set):
	writeline = "<url><loc>" + insert_set[y][0] + "</loc><lastmod>" + insert_set[y][1] + "</lastmod><changefreq>" + insert_set[y][2] + "</changefreq></url>\n"	
		
	if (maxLength + index) % 4998 != 0:
		with open(work_dir + str(sitemaptotals) + ".xml", "a") as xmlfile:
			xmlfile.write(writeline)
	if (maxLength + index) % 4998 == 0 or index +1 == len(insert_set):
		with open(work_dir + str(sitemaptotals) + ".xml", "a") as xmlfile:
			xmlfile.write(writeline)
			xmlfile.write("</urlset>\n")
		
		sitemaptotals = sitemaptotals + 1
		if index +1 != len(insert_set):
			with open(work_dir + str(sitemaptotals) + ".xml", "a") as xmlfile:
				xmlfile.write("<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n")
				xmlfile.write("<?xml-stylesheet type=\"text/xsl\" href=\"//scholarlypublications.universiteitleiden.nl/sitemap.xsl\"?>\n")
				xmlfile.write("<urlset xmlns=\"http://www.sitemaps.org/schemas/sitemap/0.9\">\n")

os.remove(handle_file)

currenttime = datetime.strftime(datetime.now(), "%Y-%m-%dT%H:%MZ")

with open(work_dir + "index.xml", "w") as xmlfile:
	xmlfile.write("<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n")
	xmlfile.write("<?xml-stylesheet type=\"text/xsl\" href=\"//scholarlypublications.universiteitleiden.nl/sitemap.xsl\"?>\n")
	xmlfile.write("<sitemapindex xmlns=\"http://www.sitemaps.org/schemas/sitemap/0.9\">\n")
	for x in range(1, sitemaptotals):
		xmlfile.write("<sitemap><loc>http://scholarlypublications.universiteitleiden.nl/sitemap.xml?page=" + str(x) + "</loc><lastmod>" + currenttime + "</lastmod></sitemap>\n")
	xmlfile.write("</sitemapindex>")

print "{0} -- Processed sitemap".format(str(datetime.now()))