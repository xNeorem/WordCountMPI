#include <sys/stat.h>
#include <stdio.h>
#include <dirent.h>
#include <string.h>
#include <ctype.h>
#include <stdlib.h>
#include "hashmap.h"

#include "mpi.h"

#define DIRNAME "./files/"
const int DIRNAME_LEN = strlen(DIRNAME);

#define FILENAME_SIZE 100
#define MAX_FILE 40
#define WORD_SIZE 30

const char delim[] = "\n  \n\r,.:;\t()\"?!";
const int delim_size = strlen(delim);

#define error_mpi(_msg_) { \
    fprintf(stderr, "Error: %s (%s:%d)\n", (_msg_), __FILE__, __LINE__); \
    MPI_Finalize(); \
    exit(1); \
}

    
typedef struct MyFile{
    char name[FILENAME_SIZE];
    long file_size;
    long index;
}MyFile;


typedef struct Word{
    char word[WORD_SIZE];
    int frequecy;
}Word;

struct mydata{
    Word *array;
    int i;
};

int word_compare(const void *a, const void *b, void *udata) {
    const Word *ua = a;
    const Word *ub = b;
    return strcmp(ua->word, ub->word);
}

bool word_iter(const void *item, void *udata) {
    const Word *word = item;
    printf("%s (frequecy=%d)\n", word->word, word->frequecy);
    return true;
}

bool word_to_array(const void *item,void *udata){
    struct mydata *data = udata;
    const Word *word = item;

    data->array[data->i++] = *word;

    return true;

}

uint64_t word_hash(const void *item, uint64_t seed0, uint64_t seed1) {
    const Word *word = item;
    return hashmap_sip(word->word, strlen(word->word), seed0, seed1);
}

int isdelim(char value){

    for(int i=0; i < delim_size; i++)
        if(value == delim[i])
            return 1;

    return 0;
}


int padding(MyFile *file, long pos,char * padding_buffer){

 
    
    FILE *stream = fopen(file->name, "rt");
    fseek(stream, pos, SEEK_SET);

    int count = 0;

    int size = fread(padding_buffer, sizeof(char), WORD_SIZE, stream);
    padding_buffer[size++] = '\0';

    
    while(!isdelim(padding_buffer[count]))
        count++;

    fclose(stream);
    // printf("%s %s %d\n",file->name,padding_buffer,count);

    return count;


    
}

int binarySearch(MyFile arr[], int l, int r, long x,int curr){
    if (r >= l) {
        int mid = l + (r - l) / 2;
        if(arr[mid].index == x)
            return mid;
        else if (arr[mid].index > x)
            return binarySearch(arr, l, mid - 1, x,mid);
        else
            return binarySearch(arr, mid + 1, r, x,mid);
    }

    // printf("%ld x > %ld\n",x,arr[curr].index);

    return (x > arr[curr].index) ? curr + 1 : curr;
}

int calc_max(int *array,int size){
    int max = array[0];

    for(int i=1; i<size; i++)
        if(max < array[i])
            max = array[i];

    return max;
}


int main(int argc, char **argv){

    //printf("size : %f",size);

    int world_size, rank;

    MPI_Init(&argc, &argv);
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    MPI_Comm_size(MPI_COMM_WORLD, &world_size);

    const MPI_Datatype types[2] = {MPI_CHAR, MPI_INT};
    const int blocklengths[2] = {WORD_SIZE,1};
    const MPI_Aint offsets[2] = {
        offsetof(Word, word),
        offsetof(Word, frequecy),
    };
    
    printf("%ld %ld\n",offsetof(Word, word),offsetof(Word, frequecy));

    MPI_Datatype MPI_MY_WORD;
    MPI_Type_create_struct(2, blocklengths, offsets, types, &MPI_MY_WORD);
    MPI_Type_commit(&MPI_MY_WORD);


    DIR *d;
    struct dirent *dir;
    struct stat st;
    long size = 0;
    d = opendir(DIRNAME);

    MyFile myFiles[MAX_FILE];
    int n = 0;


    if (!d)
        error_mpi("cannot open file's directory.")

    while ((dir = readdir(d)) != NULL){

        if (dir->d_type == DT_REG) {

            if(DIRNAME_LEN + strlen(dir->d_name) > FILENAME_SIZE)
                error_mpi("file name to long for buffer size, increment it.");
                
            strcpy(myFiles[n].name,DIRNAME);
            strcat(myFiles[n].name,dir->d_name);

            stat(myFiles[n].name, &st);
            printf("%s %ld\n", dir->d_name,st.st_size);
            myFiles[n].file_size = st.st_size;
            size += st.st_size;
            myFiles[n].index = size;
            n++;

            if(n > MAX_FILE)
                error_mpi("Too many files in directory for buffer size, increment it.");
        }
    }

    closedir(d);

    printf("size %ld\n",size);


    long byte_for_process = (size % world_size != 0) ? (size/world_size) + 1 : size/world_size;

    long start[world_size],enda[world_size];
    int padd[world_size],startIndex[world_size],endIndex[world_size];

    char *padding_buffer = malloc(sizeof(char) * (WORD_SIZE+1));
    if(!padding_buffer)
        error_mpi("Cannot allocate padding buffer.");



    for(int i=0; i < world_size; i++){

        start[i] = (i == 0) ? i*byte_for_process : enda[i-1];
        enda[i] = start[i] + byte_for_process;

        startIndex[i] = binarySearch(myFiles, 0, n - 1, start[i],0);
        endIndex[i] = binarySearch(myFiles, 0, n - 1, enda[i],0);
        

        if(enda[i] > size){
            padd[i] = size - enda[i];
            enda[i] = size;
        }else{
            long end = (endIndex[i] == 0) ? enda[i] : enda[i]-myFiles[endIndex[i]-1].index;
            padd[i] = (enda[i] > size) ? 0 : padding(&myFiles[endIndex[i]],end,padding_buffer);
        }

        enda[i] += padd[i];

        printf("rank %d p(%d) start:%ld end:%ld index : %d  index_end: %d pad %d byte %ld\n",rank,i,start[i],enda[i],startIndex[i],endIndex[i],padd[i],byte_for_process);

    }

    int index = startIndex[rank];
    int indexEnd = endIndex[rank];
    int n_file = indexEnd - index;

    long starting = (index == 0) ? start[rank] : start[rank]-myFiles[index-1].index;
    int pad = padd[rank];
    //printf("p(%d) start:%ld end:%ld index : %d  index_end: %d real_start:%ld file_size:%ld pad %d\n",rank,start[rank],enda[ran],index,indexEnd,starting,myFiles[index].index,pad);
    
    int buffer_size = (byte_for_process/sizeof(char)) + pad + n_file;
    char *buffer = malloc(buffer_size+1);
    if(!buffer)
        error_mpi("cannot allocate buffer for input file.");


    FILE * stream;
    int read_byte = 0;
    while(read_byte != buffer_size){
        stream = fopen(myFiles[index].name, "r");
        fseek(stream,starting, SEEK_SET );
        read_byte += fread(buffer+read_byte, sizeof(char), buffer_size-read_byte, stream);
        printf("P(%d) opening %s index %d starting %ld byte_read %d n_file %d\n",rank,myFiles[index].name,index,starting,read_byte,n_file);
        fclose(stream);

        if(n_file){
            buffer[read_byte++] = '\n';
            n_file--;
        }

        index++;
        starting = 0;
    }

    buffer[read_byte++] ='\0';

    // char filename[20];
    // sprintf(filename, "file%d.txt",rank);
    // FILE *fp = fopen( filename , "w" );
    // fwrite(buffer, 1 , buffer_size , fp );
    // fclose(fp);

    struct hashmap *map = hashmap_new(sizeof(Word), 0, 0, 0, 
                                     word_hash, word_compare, NULL);
                                     
    // const char delim[] = "\n  \n\r,.:;\t()\"'";
    char * token = strtok(buffer, delim);
    Word *temp,key;
    while( token != NULL ) {
        
        strcpy( key.word, token);
        temp = hashmap_get(map, &key);
        if(!temp){
            Word newWord;
            strcpy(newWord.word,token);
            newWord.frequecy = 1;
            hashmap_set(map,&newWord);
        }
        else
            temp->frequecy++;

        token = strtok(NULL, delim);

    }

    free(buffer);


    if(rank){
        int mapSize = hashmap_count(map);
        Word *to_send_array = malloc(sizeof(Word) * mapSize);
        if(!to_send_array)
            error_mpi("cannot allocate to_send_array");

        struct mydata data;
        data.array = to_send_array;
        data.i = 0;

        hashmap_scan(map,word_to_array,&data);

        
        // int sum = 0;
        // for(int i = 0; i < data.i; i++)
        //     sum += to_send_array[i].frequecy;
        // printf("P(%d) data in array : %d last elemeent %s %d sum %d\n",rank,data.i,data.array[2].word,data.array[2].frequecy,sum);

        MPI_Send(data.array, data.i, MPI_MY_WORD, 0, 1, MPI_COMM_WORLD);

        free(to_send_array);

    }else if(rank == 0){

        MPI_Status status;
        int n_items[world_size];

        n_items[0] = 0;

        for(int i=1; i < world_size; i++){
            MPI_Probe(i, 1, MPI_COMM_WORLD, &status);
            MPI_Get_count(&status, MPI_MY_WORD, &n_items[i]);
        }

        int max = calc_max(n_items,world_size);

        Word *buff = malloc(sizeof(Word) * max);
        if(!buff)
            error_mpi("cannot allocate buff in master to recive data from workers.");
        
        for(int i=1; i < world_size; i++){
            printf("from %d recived %d\n",i,n_items[i]);
            MPI_Recv(buff, n_items[i], MPI_MY_WORD, i, 1, MPI_COMM_WORLD, &status);
            for (int j=0; j < n_items[i];j++){

                temp = hashmap_get(map, &buff[j]);
                if(!temp){
                    hashmap_set(map,&buff[j]);
                }
                else
                    temp->frequecy += buff[j].frequecy;

            }
        }

        free(buff);

        int mapSize = hashmap_count(map);

        Word *to_array = malloc(sizeof(Word)*mapSize);
        if(!to_array)
            error_mpi("cannot allocate to_array in master node.");

        struct mydata data;
        data.array = to_array;
        data.i = 0;

        hashmap_scan(map,word_to_array,&data);

        int sum = 0;
        for(int i = 0; i < data.i; i++)
            sum += to_array[i].frequecy;


        printf("Mapsize: %d, total words : %d\n",mapSize,sum);

        char filename[20];
        sprintf(filename, "fileresult%d.txt",world_size);
        FILE *fp = fopen( filename , "w" );
        for(int i = 0; i < data.i; i++) 
            fprintf(fp,"%s %d\n",data.array[i].word,data.array[i].frequecy);
        //fwrite(buffer, 1 , buffer_size , fp );
        fclose(fp);

        free(to_array);
        
        //printf("\n-- iterate over all users --\n");
        //hashmap_scan(map, word_iter, NULL);



    }

    free(padding_buffer);
    MPI_Type_free(&MPI_MY_WORD);
    MPI_Finalize();

    return 0;
}




