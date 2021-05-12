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

//#define DEBUG 1
#define BENCHMARK

const char delim[] = "\n  \n\r,.:;\t()\"?!";
const int delim_size = strlen(delim);

#define error_mpi(_msg_) { \
    fprintf(stderr, "Error: %s (%s:%d)\n", (_msg_), __FILE__, __LINE__); \
    MPI_Abort(MPI_COMM_WORLD,EXIT_FAILURE); \
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

typedef struct Job{
    long int start,end;
    int startIndex, endIndex;
}Job;


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

    #ifdef DEBUG
        printf("DBG (PADDING) %s %s %d\n",file->name,padding_buffer,count);
    #endif

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

    #ifdef DEBUG
        printf("DBG (binarySearch) %ld x > %ld\n",x,arr[curr].index);
    #endif


    return (x > arr[curr].index) ? curr + 1 : curr;
}

int calc_max(int *array,int size){
    int max = array[0];

    for(int i=1; i<size; i++)
        if(max < array[i])
            max = array[i];

    return max;
}

Job* mapping_jobs(MyFile *myFiles,int file_n,int total_files_size,int world_size){

    long byte_for_process = (total_files_size % world_size != 0) ? (total_files_size/world_size) + 1 : total_files_size/world_size;

    Job *jobs = malloc(sizeof(Job) * world_size);
    if(!jobs)
        error_mpi("Cannot allocate jobs buffer.");

    int padd[world_size];

    char *padding_buffer = malloc(sizeof(char) * (WORD_SIZE+1));
    if(!padding_buffer)
        error_mpi("Cannot allocate padding buffer.");



    for(int i=0; i < world_size; i++){

        jobs[i].start = (i == 0) ? i*byte_for_process : jobs[i-1].end;
        jobs[i].end = jobs[i].start + byte_for_process;

        jobs[i].startIndex = binarySearch(myFiles, 0, file_n - 1, jobs[i].start,0);
        jobs[i].endIndex = binarySearch(myFiles, 0, file_n - 1, jobs[i].end,0);
        

        if(jobs[i].end >= total_files_size){
            padd[i] = total_files_size - jobs[i].end;
            jobs[i].endIndex = file_n - 1;
        }else{
            long end = (jobs[i].endIndex == 0) ? jobs[i].end : jobs[i].end - myFiles[jobs[i].endIndex - 1].index;
            padd[i] = padding(&myFiles[jobs[i].endIndex],end,padding_buffer);
        }

        jobs[i].end += padd[i];

    }

    free(padding_buffer);

    return jobs;
}

void reduce(struct hashmap *map, Word *words, int size){

    Word *temp;

    for (int j=0; j < size; j++){

        temp = hashmap_get(map, &words[j]);
        if(!temp){
            hashmap_set(map,&words[j]);
        }
        else
            temp->frequecy += words[j].frequecy;

    }
}


int main(int argc, char **argv){


    int world_size, rank;

    MPI_Init(&argc, &argv);

    double starttime = MPI_Wtime();

    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    MPI_Comm_size(MPI_COMM_WORLD, &world_size);

    const MPI_Datatype typesWord[2] = {MPI_CHAR, MPI_INT};
    const int blocklengthsWord[2] = {WORD_SIZE,1};
    const MPI_Aint offsetsWord[2] = {
        offsetof(Word, word),
        offsetof(Word, frequecy),
    };

    MPI_Datatype MPI_MY_WORD;
    MPI_Type_create_struct(2, blocklengthsWord, offsetsWord, typesWord, &MPI_MY_WORD);
    MPI_Type_commit(&MPI_MY_WORD);


    const MPI_Datatype typesJob[2] = {MPI_LONG, MPI_INT};
    const int blocklengthsJob[2] = {2,2};
    const MPI_Aint offsetsJob[2] = {
        offsetof(Job, start),
        offsetof(Job, startIndex),
    };

    MPI_Datatype MPI_MY_JOB;
    MPI_Type_create_struct(2, blocklengthsJob, offsetsJob, typesJob, &MPI_MY_JOB);
    MPI_Type_commit(&MPI_MY_JOB);

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

            #ifdef DEBUG
                printf("DBG P(%d) %s %ld\n",rank,dir->d_name,st.st_size);
            #endif
            myFiles[n].file_size = st.st_size;
            size += st.st_size;
            myFiles[n].index = size;
            n++;

            if(n > MAX_FILE)
                error_mpi("Too many files in directory for buffer size, increment it.");
        }
    }

    closedir(d);

    #ifdef DEBUG
        if(rank == 0)
            printf("DBG P(%d) size all file%ld\n",rank,size);
    #endif


    Job *jobs,job;
    if(rank == 0)
        jobs = mapping_jobs(myFiles,n,size,world_size);

    MPI_Scatter(jobs, 1, MPI_MY_JOB, &job, 1, MPI_MY_JOB, 0, MPI_COMM_WORLD);

    #ifdef DEBUG
        printf("DBG P(%d) start %ld end %ld startIndex %d endIndex %d\n",rank,job.start,job.end,job.startIndex,job.endIndex);
    #endif


    int index = job.startIndex;
    int indexEnd = job.endIndex;
    int n_file = indexEnd - index;

    long starting = (index == 0) ? job.start : job.start - myFiles[index-1].index;
    #ifdef DEBUG
        printf("DBG P(%d) start:%ld end:%ld index : %d  index_end: %d real_start:%ld file_size:%ld \n",rank,job.start,job.end,index,indexEnd,starting,myFiles[index].index);
    #endif
    
    int buffer_size = job.end - job.start + n_file;
    char *buffer = malloc(buffer_size+1);
    if(!buffer)
        error_mpi("cannot allocate buffer for input file.");

    FILE * stream;
    int read_byte = 0;
    while(read_byte != buffer_size){
        stream = fopen(myFiles[index].name, "r");
        fseek(stream,starting, SEEK_SET );
        read_byte += fread(buffer+read_byte, sizeof(char), buffer_size-read_byte, stream);

        #ifdef DEBUG
            printf("DBG P(%d) opening %s index %d starting %ld byte_read %d n_file %d\n",rank,myFiles[index].name,index,starting,read_byte,n_file);
        #endif

        fclose(stream);

        if(n_file){
            buffer[read_byte++] = '\n';
            n_file--;
        }

        index++;
        starting = 0;
    }

    buffer[read_byte++] ='\0';

    #ifdef DEBUG
        char filename[20];
        sprintf(filename, "file%d.txt",rank);
        FILE *fp = fopen( filename , "w" );
        fwrite(buffer, 1 , buffer_size , fp );
        fclose(fp);
    #endif

    struct hashmap *map = hashmap_new(sizeof(Word), 0, 0, 0,word_hash, word_compare, NULL);
                                     
    char * token = strtok(buffer, delim), *p;
    Word *temp,key;
    while( token != NULL ) {

        p = token;
        for ( ; *p; ++p) *p = tolower(*p);

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

        #ifdef DEBUG
            int sum = 0;
            for(int i = 0; i < data.i; i++)
                sum += to_send_array[i].frequecy;
            printf("DBG P(%d) data in array : %d last elemeent %s %d sum %d\n",rank,data.i,data.array[2].word,data.array[2].frequecy,sum);
        #endif

        MPI_Send(data.array, data.i, MPI_MY_WORD, 0, 1, MPI_COMM_WORLD);

        free(to_send_array);

    }else if(rank == 0){

        int k = 1;
        int flags[world_size],n_items[world_size];
        MPI_Status status[world_size];
        flags[0] = 0;
        int curr_buff_size = 0;
        Word *buff;

        while(k <= world_size-1)
            for(int i=1; i < world_size; i++){
                MPI_Iprobe(i, 1, MPI_COMM_WORLD, &flags[i], &status[i]);
                if(flags[i]){

                    MPI_Get_count(&status[i], MPI_MY_WORD, &n_items[i]);

                    #ifdef DEBUG
                        printf("DBG P(%d) %d size %d k %d\n",rank,i,n_items[i],k);
                    #endif

                    fflush(stdout);
                    if(n_items[i] > curr_buff_size){

                        buff = realloc(buff,n_items[i]* sizeof(Word));
                        if(!buff)
                            error_mpi("cannot allocate buff in master to recive data from workers.");

                        curr_buff_size = n_items[i];

                        }
        
                    MPI_Recv(buff, n_items[i], MPI_MY_WORD, i, 1, MPI_COMM_WORLD, &status[i]);

                    reduce(map,buff,n_items[i]);
                    k++;
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

        double endtime = MPI_Wtime();

        #ifdef BENCHMARK
        FILE *fpbm;
        fpbm = fopen("benchmark.txt", "a");
        fprintf(fpbm, "%d %f %f %f %d %ld %d \n",world_size,starttime,endtime,endtime-starttime,n,size,sum);
        #endif

        printf("word size %d Mapsize: %d total words : %d in %f\n",world_size,mapSize,sum,endtime-starttime);

        FILE *fpcsv;
        fpcsv = fopen("risultati.csv", "w"); 
        fprintf(fpcsv, "word,Frequency\n");
        for (int i = 0; i < data.i; i++)
            fprintf(fpcsv, "\"%s\",%d\n", to_array[i].word, to_array[i].frequecy);
        fclose(fpcsv);

        #ifdef DEBUG
            char filename[20];
            sprintf(filename, "fileresult%d.txt",world_size);
            FILE *fp = fopen( filename , "w" );
            for(int i = 0; i < data.i; i++) 
                fprintf(fp,"%s %d\n",data.array[i].word,data.array[i].frequecy);
            fclose(fp);
        #endif


        free(to_array);
    }

    MPI_Type_free(&MPI_MY_WORD);
    MPI_Type_free(&MPI_MY_JOB);
    MPI_Finalize();

    return 0;
}




