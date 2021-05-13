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

strong:
	for i in `seq 1 39`; do cp $(FILE_FOLDER)merged_file.txt "$(FILE_FOLDER)test_$$i.txt" ; done
	echo "Files made. starting mpirun."
	for i in `seq 1 8`; do mpirun --allow-run-as-root -np $$i $(OUT_FOLDER)$(MAIN) $(FILE_FOLDER); done
	for i in `seq 1 39`; do rm "$(FILE_FOLDER)test_$$i.txt" ; done
	echo "Files cleaned. strong done."

week:
	for i in `seq 1 8`; do cp $(FILE_FOLDER)merged_file.txt "$(FILE_FOLDER)test_$$i.txt" && mpirun --allow-run-as-root -np $$i $(OUT_FOLDER)$(MAIN) $(FILE_FOLDER); done
	for i in `seq 1 8`; do rm "$(FILE_FOLDER)test_$$i.txt" ; done
	echo "Files cleaned. week done."

benchmark: strong week
