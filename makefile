install:
	sudo apt update
	sudo apt install libssl-dev

ring:
	g++ headers/ring.hpp ring.cpp -o ring.o -lssl -lcrypto
	./ring.o $(KEY)
