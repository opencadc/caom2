#! /usr/bin/env python
# coding: utf-8

import sys
import getopt
import re
import csv

BASEURL = 'urn:UNSET'
TITLE = 'no title provided on the command line'
DESCRIPTION="""
no summary file provided on command line
"""

try:
    opts, args = getopt.getopt(sys.argv[1:],
                               "b:o:h:d:s:t",
                               ["base=", "ontology=", "html=", "date=", "summary=", "title="])
except getopt.GetoptError:
    print "Bad options"
    sys.exit(1)

if len(args) != 1:
    print "Usage: make-ontology.py --base=<URI> --ontology=<ttl file> --html=<html file> --date=<current date> --summary=<summary.html> --title=<vocab name> <input.csv>"
    sys.exit(1)
csv_input_file = args[0]

#### Header and metadata

ontology_output_file = None
html_output_file = None
date_str = None
title = None
summ = None

for o, a in opts:
    if o in ("-o" "--ontology"):
        ontology_output_file = a
    elif o in ("-h" "--html"):
        html_output_file = a
    elif o in ("-d" "--date"):
        date_str = a
    elif o in ("-b" "--base"):
        BASEURL = a
    elif o in ("-s" "--summary"):
        summ = a
    elif o in ("-t" "--title"):
        TITLE = a + ' vocabulary'
    else:
        print "Unrecognised option", o
        sys.exit(1)

if not (ontology_output_file or html_output_file):
    print "Need to specify either --html or --ontology or both"
    sys.exit(1)

if ontology_output_file:
    ont = open(ontology_output_file, 'w')

if html_output_file:
    html = open(html_output_file, 'w')

if summ:
    sf = open(summ, 'r')
    DESCRIPTION = sf.read()

if ont:
    ont.write("""@base <{}>.
@prefix : <#>.

@prefix dc: <http://purl.org/dc/terms/> .
@prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#> .
@prefix owl: <http://www.w3.org/2002/07/owl#> .
@prefix xsd: <http://www.w3.org/2001/XMLSchema#> .
@prefix rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> .
@prefix foaf: <http://xmlns.com/foaf/0.1/>.

<> a owl:Ontology;
  dc:created "{}";
  dc:creator [ foaf:name "Patrick Dowler" ];
  rdfs:label "{}"@en;
  dc:title "{}"@en;
  dc:description \"\"\"{}\"\"\".

dc:created a owl:AnnotationProperty.
dc:creator a owl:AnnotationProperty.
dc:title a owl:AnnotationProperty.
dc:description a owl:AnnotationProperty.

""".format(BASEURL, date_str, TITLE, TITLE, DESCRIPTION))

if html:
    # Yes, we're going to write out XML with print statements.
    # Yes, don't worry, I feel dirty.
    ## Yes, you should :-) PD ##
    html.write("""<html xmlns='http://www.w3.org/1999/xhtml'>
<head>
<title>{}</title>
</head>
<body>
<h1>{}</h1>
<p>This is the description of the namespace <code>{}</code> as of {}.</p>
<p>{}</p>
<p>
Alternate formats: <a href="DataProductType.rdf">RDF</a> <a href="DataProductType.ttl">TTL</a> 
</p>

<table>
<tr><th>Predicate</th><th>Parent</th><th>Label</th><th>Comment</th></tr>
""".format(TITLE, TITLE, BASEURL, date_str, DESCRIPTION, date_str, date_str))


#### Parse the CSV file

ignored_row = re.compile('^ *(#.*)?$')

with open(csv_input_file, 'r') as csvfile:
    current_parent = None
    csv = csv.reader(csvfile)
    for row in csv:
        if len(row) == 0:
            continue
        (term, level_string, label, description) = row
        if ignored_row.match(term):
            continue

        level = int(level_string)
        # I need to make the following more generic if we end up with
        # more levels in the set of predicates
        if level == 1:
            current_parent = term
            if ont:
                ont.write('<#{}> a rdf:Property'.format(term))
            if html:
                html.write('<tr><td>#{}</td><td></td>'.format(term))
        else:
            if ont:
                ont.write('<#{}> a rdf:Property;\n  rdfs:subPropertyOf <#{}>'.format(term, current_parent))
            if html:
                html.write('<tr><td>#{}</td><td>#{}</td>'.format(term, current_parent))

        if label != '':
            if ont:
                ont.write(';\n  rdfs:label "{}"'.format(label))
            if html:
                html.write('<td>{}</td>'.format(label))
        if description != '':
            if ont:
                ont.write(';\n  rdfs:comment "{}"'.format(description))
            if html:
                html.write('<td>{}</td>'.format(description))
        if ont:
            ont.write('.\n\n')
        if html:
            html.write('</tr>\n')

if html:
    html.write('</table></body></html>')
