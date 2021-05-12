CC = mpicc
HEADERS = hashmap.c
MAIN = wordcount
OUT_FOLDER = ./out/
FILE_FOLDER = ./files/
NP = 6
N = 10

compile:
	$(CC) -o $(OUT_FOLDER)$(MAIN) $(HEADERS) $(MAIN).c

clean :
	rm $(OUT_FOLDER)$(MAIN)

run: 
	mpirun --allow-run-as-root -np $(NP) $(OUT_FOLDER)$(MAIN) $(FILE_FOLDER)

benchmark:
	for i in `seq 1 10`; do mpirun --allow-run-as-root -np $$i $(OUT_FOLDER)$(MAIN) $(FILE_FOLDER); done
