//global variable for the file descriptor
extern int outFD;
extern char* colToSortOn;
extern pthread_mutex_t global_mutex;

//struct for passing args to threads
typedef struct {
    //path to next directory or file
    char* currentPath;
    //char* filename for file handler
    char* filename;
} tArgs;

void handleInputs(int argc, char** argv, char** colToSort, char** dirToSort, char** outDir);

int identifyPosition(char* columnName);

void* fileHandler(void* tArgs);

void* dirHandler(void* tArgs);

void handlerContainer(char* path);