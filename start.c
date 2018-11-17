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

int outFD;
char* colToSortOn;
pthread_mutex_t global_mutex;

int main(int argc, char** argv){

    char* outDir = NULL;
    char* dirToSort = NULL;
    handleInputs(argc, argv, &colToSortOn, &dirToSort, &outDir);

    if(dirToSort == NULL){
        dirToSort = ".";
    }

    if(outDir == NULL){
        outDir = ".";
    }

    char* pathToSort = (char*) malloc (PATH_MAX + 1);
    realpath(dirToSort, pathToSort);

    char* pathToOut = (char*) malloc (PATH_MAX + 1);
    realpath(outDir, pathToOut);

    char* allFiles = "AllFiles-sorted-";
    char* csv = ".csv";
    char* sortFileName = (char*) malloc (strlen(allFiles) + strlen(colToSortOn) + strlen(csv) + 1);
    strcpy(sortFileName, "AllFiles-sorted-");
    strcat(sortFileName, colToSortOn);
    strcat(sortFileName, csv);
    
    if(pthread_mutex_init(&global_mutex, NULL) != 0) { 
        printf("\n mutex init has failed\n"); 
        return 0; 
    } 

    char* pathToOutFile = (char*) malloc (strlen(pathToOut) + strlen(sortFileName) + 2);
    strcpy(pathToOutFile, pathToOut);
    strcat(pathToOutFile, "/");
    strcat(pathToOutFile, sortFileName);

    int check = open(pathToOutFile, O_RDONLY);
    if(check != -1){
        close(check);
        printf("Sorted file for this column already exists.\n");
        exit(0);
    }

    outFD = open(pathToOutFile, O_CREAT | O_RDWR | O_EXCL, S_IRUSR | S_IWUSR | S_IXUSR);
    char* firstLine = "color,director_name,num_critic_for_reviews,duration,director_facebook_likes,actor_3_facebook_likes,actor_2_name,actor_1_facebook_likes,gross,genres,actor_1_name,movie_title,num_voted_users,cast_total_facebook_likes,actor_3_name,facenumber_in_poster,plot_keywords,movie_imdb_link,num_user_for_reviews,language,country,content_rating,budget,title_year,actor_2_facebook_likes,imdb_score,aspect_ratio,movie_facebook_likes\n";
    write(outFD, firstLine, strlen(firstLine));

    handlerContainer(pathToSort);

    close(outFD);
    pthread_mutex_destroy(&global_mutex);
}