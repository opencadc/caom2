
# Set the timestamp to the current date.
TIMESTAMP := $(shell date -u +%Y-%m-%d)

# The 'filename' of the distributed URLs.
# This controls the file which is actually served by the eventual web
# server, but it is not itself published (ie, it's an implementation
# detail, of a sort), so it's safe to change it between releases if
# that is for some reason convenient.
VOC1=DataProductType
VOC2=ProductType
VOC3=TargetType
VOC4=Quality
VOC5=Status

# Base URI of the distributed ontology.
# The 'core' ontology is ${BASEURI}/core -- this URL is published as
# part of an IVOA standard, so don't change it lightly
BASEURI=http://www.opencadc.org/caom2

docs: voc1 voc2 voc3 voc4 voc5

voc1: build/html/${VOC1}/${VOC1}.rdf

voc2: build/html/${VOC2}/${VOC2}.rdf

voc3: build/html/${VOC3}/${VOC3}.rdf

voc4: build/html/${VOC4}/${VOC4}.rdf

voc5: build/html/${VOC5}/${VOC5}.rdf

build/html/${VOC1}/${VOC1}.ttl: make-ontology.py src/main/${VOC1}/terms.csv
	mkdir -p build/html/${VOC1}
	rm -f $@
	python make-ontology.py \
	  --base=${BASEURI}/${VOC1} \
	  --date=${TIMESTAMP} \
	  --ontology=tmp.ttl \
	  --html=build/html/${VOC1}/${VOC1}.html \
	  --title=${VOC1} \
	  --summary=src/main/${VOC1}/description.html \
	  src/main/${VOC1}/terms.csv && mv tmp.ttl $@

build/html/${VOC2}/${VOC2}.ttl: make-ontology.py src/main/${VOC2}/terms.csv
	mkdir -p build/html/${VOC2}
	rm -f $@
	python make-ontology.py \
	  --base=${BASEURI}/${VOC2} \
	  --date=${TIMESTAMP} \
	  --ontology=tmp.ttl \
	  --html=build/html/${VOC2}/${VOC2}.html \
	  --title=${VOC2} \
	  --summary=src/main/${VOC2}/description.html \
	  src/main/${VOC2}/terms.csv && mv tmp.ttl $@

build/html/${VOC3}/${VOC3}.ttl: make-ontology.py src/main/${VOC3}/terms.csv
	mkdir -p build/html/${VOC3}
	rm -f $@
	python make-ontology.py \
	  --base=${BASEURI}/${VOC3} \
	  --date=${TIMESTAMP} \
	  --ontology=tmp.ttl \
	  --html=build/html/${VOC3}/${VOC3}.html \
	  --title=${VOC3} \
	  --summary=src/main/${VOC3}/description.html \
	  src/main/${VOC3}/terms.csv && mv tmp.ttl $@

build/html/${VOC4}/${VOC4}.ttl: make-ontology.py src/main/${VOC4}/terms.csv
	mkdir -p build/html/${VOC4}
	rm -f $@
	python make-ontology.py \
	  --base=${BASEURI}/${VOC4} \
	  --date=${TIMESTAMP} \
	  --ontology=tmp.ttl \
	  --html=build/html/${VOC4}/${VOC4}.html \
	  --title=${VOC4} \
	  --summary=src/main/${VOC4}/description.html \
	  src/main/${VOC4}/terms.csv && mv tmp.ttl $@

build/html/${VOC5}/${VOC5}.ttl: make-ontology.py src/main/${VOC5}/terms.csv
	mkdir -p build/html/${VOC5}
	rm -f $@
	python make-ontology.py \
	  --base=${BASEURI}/${VOC5} \
	  --date=${TIMESTAMP} \
	  --ontology=tmp.ttl \
	  --html=build/html/${VOC5}/${VOC5}.html \
	  --title=${VOC5} \
	  --summary=src/main/${VOC5}/description.html \
	  src/main/${VOC5}/terms.csv && mv tmp.ttl $@

# Convert the generated Turtle to RDF/XML.
# The following only works if rapper is installed <http://librdf.org>.
build/html/${VOC1}/${VOC1}.rdf: build/html/${VOC1}/${VOC1}.ttl
	mkdir -p build/html/${VOC1}
	rm -f $@
	rapper -iturtle -ordfxml-abbrev build/html/${VOC1}/${VOC1}.ttl >tmp.rdf && mv tmp.rdf $@

build/html/${VOC2}/${VOC2}.rdf: build/html/${VOC2}/${VOC2}.ttl
	mkdir -p build/html/${VOC2}
	rm -f $@
	rapper -iturtle -ordfxml-abbrev build/html/${VOC2}/${VOC2}.ttl >tmp.rdf && mv tmp.rdf $@

build/html/${VOC3}/${VOC3}.rdf: build/html/${VOC3}/${VOC3}.ttl
	mkdir -p build/html/${VOC3}
	rm -f $@
	rapper -iturtle -ordfxml-abbrev build/html/${VOC3}/${VOC3}.ttl >tmp.rdf && mv tmp.rdf $@

build/html/${VOC4}/${VOC4}.rdf: build/html/${VOC4}/${VOC4}.ttl
	mkdir -p build/html/${VOC4}
	rm -f $@
	rapper -iturtle -ordfxml-abbrev build/html/${VOC4}/${VOC4}.ttl >tmp.rdf && mv tmp.rdf $@

build/html/${VOC5}/${VOC5}.rdf: build/html/${VOC5}/${VOC5}.ttl
	mkdir -p build/html/${VOC5}
	rm -f $@
	rapper -iturtle -ordfxml-abbrev build/html/${VOC5}/${VOC5}.ttl >tmp.rdf && mv tmp.rdf $@

# the 'check' target does a very basic sanity-check on the generated RDF/XML
# (this checks we haven't messed up path levels, nor got the .ttl @base setting wrong)
check: build/html/${VOC1}/${VOC1}.rdf build/html/${VOC2}/${VOC2}.rdf
	rapper -irdfxml -ontriples build/html/${VOC1}/${VOC1}.rdf \
	  | grep -q '${BASEURI}/${VOC1}#catalog.*type.*#Property' \
	  && echo '${VOC1}: sanity-check OK' || echo '${VOC1}: sanity-check FAILED!'
	rapper -irdfxml -ontriples build/html/${VOC2}/${VOC2}.rdf \
	  | grep -q '${BASEURI}/${VOC2}#science.*type.*#Property' \
	  && echo '${VOC2}: sanity-check OK' || echo '${VOC2}: sanity-check FAILED!'
	rapper -irdfxml -ontriples build/html/${VOC3}/${VOC3}.rdf \
	  | grep -q '${BASEURI}/${VOC3}#science.*type.*#Property' \
	  && echo '${VOC3}: sanity-check OK' || echo '${VOC3}: sanity-check FAILED!'
	rapper -irdfxml -ontriples build/html/${VOC4}/${VOC4}.rdf \
	  | grep -q '${BASEURI}/${VOC4}#science.*type.*#Property' \
	  && echo '${VOC4}: sanity-check OK' || echo '${VOC4}: sanity-check FAILED!'
	rapper -irdfxml -ontriples build/html/${VOC5}/${VOC5}.rdf \
	  | grep -q '${BASEURI}/${VOC5}#science.*type.*#Property' \
	  && echo '${VOC5}: sanity-check OK' || echo '${VOC5}: sanity-check FAILED!'

clean:
	rm -Rf build/html/${VOC1} build/html/${VOC2} tmp.* build/html/${VOC3} tmp.* \
	  build/html/${VOC4} tmp.* build/html/${VOC5} tmp.*
