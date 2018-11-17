#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <pthread.h>
#include <dirent.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>
#include <fcntl.h>
#include <libgen.h>
#include "header.h"

#define BASE_SIZE 2000

pthread_mutex_t global_mutex;

//struct for the maintanence of thread ids so that a directory isnt finished processing until
//all threads have done their duties for their couuntry
typedef struct thid {
    pthread_t threadid;
    struct thid* next;
} tidList;

//struct to hold a line from a file, holding each column's entry individually in a char**
//also holds a char* field for the thing to sort on
typedef struct line {
    char** lineEntries;
    char* sortOn;
    struct line * next;
} entryLine;

//global list for all the lines that did not have the column to sort on!
entryLine * nullList = NULL;
entryLine * nullListEnd = NULL;

//global list for all the lines which have the column to sort on!!!
entryLine * sortOnList = NULL;
entryLine * sortOnListEnd = NULL;

//make a list for all the thread ids that are spawned for going through directories
tidList* list = NULL;

//checks if a directory
int isDirectory(const char *path) {
    struct stat path_stat;
    stat(path, &path_stat);
    return S_ISDIR(path_stat.st_mode);
}

//add to the global thread id list
void addID(pthread_t tid){
    tidList* new = (tidList*) malloc (sizeof(tidList));
    new -> threadid = tid;
    new -> next = list;
    list = new; 
}

//identifies data type of col type, and if entered column is even valid
char* columnTypeIdent(char* columnName){
	if(strcmp("color", columnName) == 0 ||
		strcmp("director_name", columnName) == 0 ||
		strcmp("actor_2_name", columnName) == 0 ||
		strcmp("genres", columnName) == 0 ||
		strcmp("actor_1_name", columnName) == 0 ||
		strcmp("movie_title", columnName) == 0 ||
		strcmp("actor_3_name", columnName) == 0 ||
		strcmp("plot_keywords", columnName) == 0 ||
		strcmp("movie_imdb_link", columnName) == 0 ||
		strcmp("language", columnName) == 0 ||
		strcmp("country", columnName) == 0 ||
		strcmp("content_rating", columnName) == 0){
		
		return "string";
	}
	else if(strcmp("director_facebook_likes", columnName) == 0 ||
		strcmp("num_critic_for_reviews", columnName) == 0 ||
		strcmp("actor_3_facebook_likes", columnName) == 0 ||
		strcmp("actor_1_facebook_likes", columnName) == 0 ||
		strcmp("num_voted_users", columnName) == 0 ||
		strcmp("cast_total_facebook_likes", columnName) == 0 ||
		strcmp("facenumber_in_poster", columnName) == 0 ||
		strcmp("num_user_for_reviews", columnName) == 0 ||
		strcmp("title_year", columnName) == 0 ||
		strcmp("actor_2_facebook_likes", columnName) == 0 ||
		strcmp("duration", columnName) == 0 ||
		strcmp("movie_facebook_likes", columnName) == 0){
		
		return "int";	
	}
	else if(strcmp("aspect_ratio", columnName) == 0 ||
	        strcmp("imdb_score", columnName) == 0 ||
	        strcmp("budget", columnName) == 0 ||
	        strcmp("gross", columnName) == 0){
	
		return "float";
	}
	else{
	    return "err";
	}
}

int checkIfExists(char* directory){

    DIR *dr = opendir(directory);
    if(dr){
        //this directly exists!
        closedir(dr);
        return 1;
    }
    else{
        //this directory doesn't exist!
        return 0;
    }
}

//handles command line inputs for sorter program
void handleInputs(int argc, char** argv, char** colToSort, char** dirToSort, char** outDir){
    //must be at least exec, -c, and column name to sort
    if(argc < 3){
        printf("Too few arguments entered.\n");
        exit(0);
    }
    //either a flag was not entered or its specifier, ex. ./test -c column -d
    else if(argc == 4 || argc == 6){
        printf("Invalid number of arguments entered.\n");
        exit(0);
    }
    else if(argc > 7){
        printf("Too many arguments entered.\n");
        exit(0);
    }

    int ccount = 0; int dcount = 0; int ocount = 0;

    if(argc == 3){
        if(strcmp(argv[1], "-c") != 0){
            printf("Missing the required -c flag.\n");
            exit(0);
        }
        *colToSort = argv[2];
        ccount++;
    }

    if(argc == 5){
        if(strcmp(argv[1], "-c") == 0){
            ++ccount;
            *colToSort = argv[2];
        }
        else if(strcmp(argv[1], "-d") == 0){
            ++dcount;
            *dirToSort = argv[2];
        }
        else if(strcmp(argv[1], "-o") == 0){
            ++ocount;
            *outDir = argv[2];
        }
        else{
            printf("Invalid flag enetered.\n");
            exit(0);
        }

        if(strcmp(argv[3], "-c") == 0){
            ++ccount;
            *colToSort = argv[4];
        }
        else if(strcmp(argv[3], "-d") == 0){
            ++dcount;
            *dirToSort = argv[4];
        }
        else if(strcmp(argv[3], "-o") == 0){
            ++ocount;
            *outDir = argv[4];
        }
        else{
            printf("Invalid flag enetered.\n");
            exit(0);
        }

    }

    if(argc == 7){
        if(strcmp(argv[1], "-c") == 0){
            ++ccount;
            *colToSort = argv[2];
        }
        else if(strcmp(argv[1], "-d") == 0){
            ++dcount;
            *dirToSort = argv[2];
        }
        else if(strcmp(argv[1], "-o") == 0){
            ++ocount;
            *outDir = argv[2];
        }
        else{
            printf("Invalid flag enetered.\n");
            exit(0);
        }

        if(strcmp(argv[3], "-c") == 0){
            ++ccount;
            *colToSort = argv[4];
        }
        else if(strcmp(argv[3], "-d") == 0){
            ++dcount;
            *dirToSort = argv[4];
        }
        else if(strcmp(argv[3], "-o") == 0){
            ++ocount;
            *outDir= argv[4];
        }
        else{
            printf("Invalid flag enetered.\n");
            exit(0);
        }

        if(strcmp(argv[5], "-c") == 0){
            ++ccount;
            *colToSort = argv[6];
        }
        else if(strcmp(argv[5], "-d") == 0){
            ++dcount;
            *dirToSort = argv[6];
        }
        else if(strcmp(argv[5], "-o") == 0){
            ++ocount;
            *outDir = argv[6];
        }
        else{
            printf("Invalid flag enetered.\n");
            exit(0);
        }

    }
    if(ccount == 0){
        printf("Missing the mandatory -c flag.\n");
        exit(0);
    }

    if(ccount > 1 || dcount > 1 || ocount > 1){
        printf("A flag has been entered multiple times.\n");
        exit(0);
    }

    if(strcmp(columnTypeIdent(*colToSort), "err") == 0){
        printf("Column name entered is not a valid column name to sort on.\n");
        exit(0);
    }

    if(*dirToSort != NULL){
        if(!checkIfExists(*dirToSort)){
            printf("Directory to sort specified does not exist.\n");
            exit(0);
        }
    }

    if(*outDir != NULL){
        if(!checkIfExists(*outDir)){
            printf("The output directory specified does not exist.\n");
            exit(0);
        }
    }
}

//returns an integer based on what place the column head would be in header line of final csv
int identifyPosition(char* columnName){
    if(strcmp("color", columnName) == 0){
        return 0;
    }
    else if(strcmp("director_name", columnName) == 0){
        return 1;
    }
    else if(strcmp("num_critic_for_reviews", columnName) == 0){
        return 2;
    }
    else if(strcmp("duration", columnName) == 0){
        return 3;
    }
    else if(strcmp("director_facebook_likes", columnName) == 0){
        return 4;
    }
    else if(strcmp("actor_3_facebook_likes", columnName) == 0){
        return 5;
    }
    else if(strcmp("actor_2_name", columnName) == 0){
        return 6;
    }
    else if(strcmp("actor_1_facebook_likes", columnName) == 0){
        return 7;
    }
    else if(strcmp("gross", columnName) == 0){
        return 8;
    }
    else if(strcmp("genres", columnName) == 0){
        return 9;
    }
    else if(strcmp("actor_1_name", columnName) == 0){
        return 10;
    }
    else if(strcmp("movie_title", columnName) == 0){
        return 11;
    }
    else if(strcmp("num_voted_users", columnName) == 0){
        return 12;
    }
    else if(strcmp("cast_total_facebook_likes", columnName) == 0){
        return 13;
    }
    else if(strcmp("actor_3_name", columnName) == 0){
        return 14;
    }
    else if(strcmp("facenumber_in_poster", columnName) == 0){
        return 15;
    }
    else if(strcmp("plot_keywords", columnName) == 0){
        return 16;
    }
    else if(strcmp("movie_imdb_link", columnName) == 0){
        return 17;
    }
    else if(strcmp("num_user_for_reviews", columnName) == 0){
        return 18;
    }
    else if(strcmp("language", columnName) == 0){
        return 19;
    }
    else if(strcmp("country", columnName) == 0){
        return 20;
    }
    else if(strcmp("content_rating", columnName) == 0){
        return 21;
    }
    else if(strcmp("budget", columnName) == 0){
        return 22;
    }
    else if(strcmp("title_year", columnName) == 0){
        return 23;
    }
    else if(strcmp("actor_2_facebook_likes", columnName) == 0){
        return 24;
    }
    else if(strcmp("imdb_score", columnName) == 0){
        return 25;
    }
    else if(strcmp("aspect_ratio", columnName) == 0){
        return 26;
    }
    else if(strcmp("movie_facebook_likes", columnName) == 0){
        return 27;
    }
}

void writeToFile(entryLine * listArray, int listCount){
    int i, j;
    for(i = 0; i < listCount; i++){
        char* toWrite = (char*) malloc (2500 * sizeof(char));
        for(j = 0; j < 28; j++){
            if(j == 27){
                strcat(toWrite, listArray[i].lineEntries[j]);
                strcat(toWrite, "\n");
            }
            else{
                strcat(toWrite, listArray[i].lineEntries[j]);
                strcat(toWrite, ",");
            }
        }
        write(outFD, toWrite, strlen(toWrite));
    }
}

int countList(entryLine * ptr){
    int count = 0;
    while(ptr != NULL){
        count++;
        ptr = ptr -> next;
    }
    return count;
}

void merge(entryLine* arr, int l, int m, int r, int(*comparatorFnPtr)(char*, char*)){
    int i, j, k;
    int n1 = m - l + 1;
    int n2 =  r - m;

    /* create temp arrays */
    entryLine* L = (entryLine*) malloc(sizeof(entryLine)*n1);
    entryLine* R = (entryLine*) malloc(sizeof(entryLine)*n2);

    /* Copy data to temp arrays L[] and R[] */
    for (i = 0; i < n1; i++)
        L[i] = arr[l + i];
    for (j = 0; j < n2; j++)
        R[j] = arr[m + 1+ j];

    /* Merge the temp arrays back into arr[l..r]*/
    i = 0; // Initial index of first subarray
    j = 0; // Initial index of second subarray
    k = l; // Initial index of merged subarray
    while (i < n1 && j < n2)
    {
        if (comparatorFnPtr(L[i].sortOn,R[j].sortOn) < 0)
        {
            arr[k] = L[i];
            i++;
        }
        else
        {
            arr[k] = R[j];
            j++;
        }
        k++;
    }

    /* Copy the remaining elements of L[], if there
       are any */
    while (i < n1)
    {
        arr[k] = L[i];
        i++;
        k++;
    }

    /* Copy the remaining elements of R[], if there
       are any */
    while (j < n2)
    {
        arr[k] = R[j];
        j++;
        k++;
    }
}

void mergesort(entryLine* arr, int l, int r, int(*comparatorFnPtr)(char*, char*)){
    if (l < r)
    {
        // Same as (l+r)/2, but avoids overflow for
        // large l and h
        int m = l+(r-l)/2;

        // Sort first and second halves
        mergesort(arr, l, m, comparatorFnPtr);
        mergesort(arr, m+1, r, comparatorFnPtr);
        merge(arr, l, m, r, comparatorFnPtr);
    }
}

int comparator_INT(char* a, char* b){
  return (atoi(a)- atoi(b));
}

int comparator_FLT(char* a, char* b){
	if(atof(a) < atof(b)){
  		return -1;
  	}
	else if(atof(a) > atof(b)){
		return 1;
	}
	else{
		return 0;
	}
}

int comparator_STR(char* a, char* b){
  return strcmp(a, b);
}

void handlerContainer(char* path){
    //for printing all the thread ids cause i dont wanna make list global extern
    dirHandler((void*) path);

    //sets a ptr on the global null list and a couunter for counting up list for array declaration
    entryLine * lptr = nullList;
    int listCount = countList(lptr);

    //sets a ptr on the global non null list and counts all its stuff
    lptr = sortOnList;
    listCount += countList(lptr);

    //makes an entrylist array for storing all the struct of the stuff so we can 
    //sort it using mergesort since ours doesnt work on a linked list
    entryLine* lineArray = (entryLine*) malloc (listCount * sizeof(entryLine));
    entryLine * theGreatEmancipator = NULL;
    lptr = nullList;
    int counter = 0;
    //put all the list stuff into the array, nulls going in first of course gotta be chivalrous
    while(lptr != NULL){
        lineArray[counter] = *lptr;
        counter++;
        theGreatEmancipator = lptr;
        lptr = lptr -> next;
        free(theGreatEmancipator);
    }
    lptr = sortOnList;
    while(lptr != NULL){
        lineArray[counter] = *lptr;
        counter++;
        theGreatEmancipator = lptr;
        lptr = lptr -> next;
        free(theGreatEmancipator);
    }

    //call mergesort on our array of structs, use the corresponding function ptr based on the hecking whatever on column to sort on specified
    if(strcmp("string", columnTypeIdent(colToSortOn)) == 0){
		int (*fn)(char*, char*) = comparator_STR;
		mergesort(lineArray, 0, listCount - 1, fn);
	}
	else if(strcmp("int", columnTypeIdent(colToSortOn)) == 0){
		int (*fn)(char*, char*) = comparator_INT;
		mergesort(lineArray, 0, listCount - 1, fn);
	}
	else{
		int (*fn)(char*, char*) = comparator_FLT;
		mergesort(lineArray, 0, listCount - 1, fn);
	}

    //writes sorted array into the final csv
    writeToFile(lineArray, listCount);

    tidList* tptr = list;
    int threadCount = 0;
    printf("TIDS of all spawned threads: ");
    while(tptr != NULL){
        printf("%ld, ", tptr -> threadid);
        threadCount++;
        tptr = tptr -> next;
    }
    printf("\nTotal number of threads: %d\n", threadCount);
}

char* trimmer(char* toTrim){
    while(toTrim[0] == ' ' || toTrim[0] == '"'){
        toTrim++;
    }
    char* ptr = toTrim + strlen(toTrim) - 1;
    while(ptr > toTrim && (ptr[0] == ' ' || ptr[0] == '"')){
    	ptr--;
    }
    ptr[1] = '\0';
    return toTrim;
}

//returns an array representing lines of a file
entryLine* readInFile(int readfd){
    //base size 
    int baseSizeStringArr = 500;
    //an int array for keeping track of what columns this csv file has and does not have
    //will be size 28 for all the columns necessary, will have values for columns present,
    //NULL otherwise
    int columnsPresent[28];
    int i;
    for(i = 0; i < 28; i++){
        //cant go up too 100 so if it == 100, then you know the column is not in the file
        columnsPresent[i] = 100;
    }
    char c;
    //read in the first line and examine each column
    char* buffer = (char*) malloc (baseSizeStringArr * sizeof(char));
    int index = 0;
    int maintenance = 0;
    //keep track of commas in first line
    int firstLineCommaCount = 0;
    int emptiness = 0;
    while(read(readfd, &c, 1)){
        emptiness++;
        if(c == '\000'){
            char* err = "CSV is empty.\n";
            write(2, err, strlen(err));
            pthread_exit(NULL);
        }
        if(c == ',' || c == '\n' || c == 13){
            //null terminate the string and identify the column, and then identify place in array
            buffer[index] = '\0';
            if(strcmp(columnTypeIdent(buffer), "err") == 0){
                char* err = "CSV contains an invalid column in header.\n";
                write(2, err, strlen(err));
                pthread_exit(NULL);
            }
            //from here, we have a valid column type
            int place = identifyPosition(buffer);
            //sets the columnsPresent array with the index to place the string in for l8r
            columnsPresent[maintenance] = place;
            maintenance++;
            index = 0;
            if(c == ','){
                firstLineCommaCount++;
            }
            if(c == '\n' || c == 13){
                break;
            }
        }
        else{
            //read the character into the buffer one at a time
            buffer[index] = c;
            index++;
        }
    }
    if(emptiness == 0){
        char* err = "CSV is empty.\n";
        write(2, err, strlen(err));
        pthread_exit(NULL);
    }

    //get past random chars at end of line to get to first of next line
    while((c == 13 || c == 10) && read(readfd, &c, 1)){}

    //allocate for an linked list of structs to hold lines and their column of interest
    entryLine * listOfLines = NULL;
    entryLine * listOfLinesEnd = NULL;
    //index to maintain where you are in the list of listOfLines
    int thisLineCommaCount = 0;
    int quoteSeen = 0;
    int interest = identifyPosition(colToSortOn);
    int set = 0;
    //now we read in each line, resetting process everytime we hit a newline character
    do {
        maintenance = 0;
        thisLineCommaCount = 0;
        //create the struct to hold the line
        entryLine * currentLine = (entryLine*) malloc (sizeof(entryLine));
        //initilize its stuff with base sizes, realloc later if necessary
        currentLine -> lineEntries = (char**) malloc (28 * sizeof (char*));
        for(i = 0; i < 28; i++){
            currentLine -> lineEntries[i] = "\0";
        }
        //the input will be trimmed and then copied into here if it is even in the CSV, otherwise NULL        
        currentLine -> sortOn = "\0";
        //the line ends with the carriage return or the newline (idk man)
        //so dont include those in the thing
        while(1){
            //if a comma or newline is seen that is not in quotes, 
            //its the end of a word, process it and store
            if((c == ',' && !quoteSeen) || c == '\n' || c == 13){
                if(c == ','){
                    thisLineCommaCount++;
                }
                buffer[index] = '\0';
                if(columnsPresent[maintenance] == interest && set == 0){
                    set++;
                    char trimmed[500];
                    strcpy(trimmed, buffer);
                    char* trimmy = trimmed;
                    trimmy = trimmer(trimmy);
                    currentLine -> sortOn = (char*) malloc (500 * sizeof(char));
                    strcpy(currentLine -> sortOn, trimmy);
                }
                //copy the buffer into the corresponding place
                currentLine -> lineEntries[columnsPresent[maintenance]] = (char*) malloc (500 * sizeof(char));
                strcpy(currentLine -> lineEntries[columnsPresent[maintenance]], buffer);
                maintenance++;
                index = 0;
                if(c == 10 || c == 13){
                    break;
                }
            }
            //current character is not a comma or end of line, store character into buffer
            else{
                if(c == '"' && !quoteSeen){
                    quoteSeen = 1;
                }
                else if(c == '"' && quoteSeen){
                    quoteSeen = 0;
                }
                buffer[index] = c;
                index++;
            }
            read(readfd, &c, 1);
        }
        if(thisLineCommaCount != firstLineCommaCount){
            char* err = "CSV has line with invalid row entries.\n";
            write(2, err, strlen(err));
            pthread_exit(NULL);
        }
        //insert struct just made into the list of lines at the end
        currentLine -> next = NULL;
        if(listOfLines == NULL){
            listOfLines = currentLine;
            listOfLinesEnd = listOfLines;
        }
        else{
            listOfLinesEnd -> next = currentLine;
            listOfLinesEnd = currentLine;
        }
        thisLineCommaCount = 0;
        maintenance = 0;
        index = 0;
        set = 0;
        while(c == 13 && read(readfd, &c, 1)){}
    } while(read(readfd, &c, 1));
    //return the freshly off the presses list of lines wow such great get your lines here so cheap oh my god im doing this for free
    return listOfLines;
}

void addToNull(entryLine* ptr, entryLine* ptrTail){
    if(nullList == NULL){
        nullList = ptrTail;
        nullListEnd = nullList;
        ptrTail -> next = NULL;
    }
    else{
        nullListEnd -> next = ptrTail;
        ptrTail -> next = NULL;
        nullListEnd = ptrTail;
    }
}

void addToNotNull(entryLine* ptr, entryLine* ptrTail){
    if(sortOnList == NULL){
        sortOnList = ptrTail;
        sortOnListEnd = sortOnList;
        ptrTail -> next = NULL;
    }
    else{
        sortOnListEnd -> next = ptrTail;
        ptrTail -> next = NULL;
        sortOnListEnd = ptrTail;
    }
}

int isCSV(char* ptr){
    if(strlen(ptr) < 5){
        return 0;
    }
    char* ptrptr = ptr + (strlen(ptr) - 3);
    if(strcmp(ptrptr, "csv") == 0){
        return 1;
    }
    else{
        return 0;
    }
}

int checkIfSortedFile(char* csvFile){
    int isASortedFile = 0;
    char cpy[strlen(csvFile) + 1];
    strcpy(cpy, csvFile);
    char* token;
    token = strtok(cpy, "-");
    while(token != NULL){
        if(strcmp(token, "sorted") == 0){
            isASortedFile = 1;
        }
		token = strtok(NULL, "-");
    }
    return isASortedFile;
}

void* fileHandler(void* args){
    char* p = (char*) args;
    char * path = (char*) malloc (500);
    strcpy(path, p);
    //check if a csv
    if(!isCSV(basename(path))){
        char* err = "Encountered a non-CSV file.\n";
        write(2, err, strlen(err));
        pthread_exit(NULL);
    }

    if(checkIfSortedFile(path)){
        char* err = "File found has already been sorted.\n";
        write(2, err, strlen(err));
        pthread_exit(NULL);
    }
    //making a new path to the file in output directory
    int readfd = open(path, O_RDONLY);

    //read stuff with a dangerously long method
    entryLine* allTheLines = readInFile(readfd);
    close(readfd);
    //we now have a linked list of all the lines from this file and now we add to global LL
    entryLine* ptr = allTheLines;
    entryLine* ptrTail = NULL;
   
    pthread_mutex_lock(&global_mutex);
    //THE much fabled.............critical section!
    while(ptr != NULL){
        ptrTail = ptr;
        ptr = ptr -> next;
        if(ptrTail -> sortOn[0] == '\0'){
            //add to the end of the null list
            addToNull(ptr, ptrTail);
        }
        else{
            //add to the end of the null list
            addToNotNull(ptr, ptrTail);
        }
    }
    pthread_mutex_unlock(&global_mutex);

    //close the things
    close(readfd);
}

void* dirHandler(void* args){
    //list to maintain pids
    tidList* list = NULL;
    //passed arguments
    char* arg = (char*) args;
    char * pt = (char*) malloc (300);
    strcpy(pt, args);
    DIR* dr = opendir(pt);
    struct dirent* de;
    while ((de = readdir(dr)) != NULL){
        //check if its a directory unless it is . or ..
        if(strcmp(de -> d_name, ".") == 0 || strcmp(de -> d_name, "..") == 0){
            continue;
        }
        char * pathToNext = (char*) malloc (strlen(pt) + strlen(de -> d_name) + 2);
        strcpy(pathToNext, pt);
        strcat(pathToNext, "/");
        strcat(pathToNext, de -> d_name);
        if(isDirectory(pathToNext)){
            //create a new thread to handle the directory
            pthread_t threadid;
            pthread_create(&threadid, NULL, &dirHandler, (void*) pathToNext);
            addID(threadid);
            pthread_join(threadid, NULL);
        }
        else if(!isDirectory(pathToNext)){
            //create a new thread to handle the file
            pthread_t threadid;
            pthread_create(&threadid, NULL, &fileHandler, (void*) pathToNext);
            addID(threadid);
            tidList* number = (tidList*) malloc (sizeof(tidList));
            number -> threadid = threadid;
            number -> next = list;
            list = number;
        }
    }
    //wait for all threads to finish up their business
    tidList* ptr = list;
    while(ptr != NULL){
        pthread_join(ptr -> threadid, NULL);
        ptr = ptr -> next;
    }
}