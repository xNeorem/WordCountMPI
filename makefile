CC = mpicc
HEADERS = hashmap.c
MAIN = wordcount
OUT_FOLDER = ./out/
NP = 3

compile all:
	$(CC) -o $(OUT_FOLDER)$(MAIN) $(HEADERS) $(MAIN).c

clean :
	rm $(OUT_FOLDER)$(MAIN)

run all: 
	mpirun --allow-run-as-root -np $(NP) $(OUT_FOLDER)$(MAIN)
