CC = mpicc
HEADERS = hashmap.c
MAIN = wordcount
OUT_FOLDER = ./out/
NP = 20

compile:
	$(CC) -o $(OUT_FOLDER)$(MAIN) $(HEADERS) $(MAIN).c

clean :
	rm $(OUT_FOLDER)$(MAIN)

run: 
	mpirun --allow-run-as-root -np $(NP) $(OUT_FOLDER)$(MAIN)
