.PHONY: all clean

all:
	javac -cp . *.java
	mkdir -p ./data

clean:
	rm -f ./data.index ./data/*