DIAGRAMS := $(wildcard *.puml)
IMAGES := $(DIAGRAMS:.puml=.png)

all: $(IMAGES)

%.png: %.puml
	run-plantuml.sh $< $@

clean:
	rm -f $(IMAGES)
