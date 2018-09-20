#include "storage_mgr.h"
#include "dberror.h"
#include<stdio.h>
#include<stdlib.h>
#include<string.h>


// Initializing the storage manager here, this function displays a message to show that storage manager has started
extern void initStorageManager(){
// printing an interactive message indicating that the storage manager has started.
printf("Initiating the StorageManager .....");
}

//createPageFile creates a new page in memory for a given file name, the file name is a character string passed as a parameter to the function.
//The function returns type RC, type definition of RC is in  dberror.h 
//This function creates a page file of size 4096 bytes using the file open functions
extern RC createPageFile(char* filename)
{
	FILE *f;// creating a file pointer based on the input file name.
	int i=0,rc=0;
	char data[4096];//defining a block of data to be written.
	//Using fopen to open the file. The file is opened in write mode to enable writing data to the file.
	f = fopen(filename,"w");
	//We intend to create a file with null characters, therefore we are setting the data block in the memory with '\0' characters
        memset(&data, '\0', 4096);
	if(f==NULL) {
	//If the value of f is NULL it implies that 'fopen' did not function as expected.
	//     	printf("Could not open the file");
		// We return an error code indicating file not found if the value of 'f' is NULL. The error code is defined in dberror.h
        	return RC_FILE_NOT_FOUND;
        } else {
		//This branch of the condition executes if we could successfully open the file.
			//We use fwrite function to write the data into the file.
        		rc=fwrite((const char*)data,sizeof(data),1,f);
        
        //	printf("Number of bytes in the file %d", rc);
		//We return an error code RC_OK indicating that everything went fine. This return code is defined in dberror.h
        	return RC_OK;
	}
}


// This function opens an existing file. The file information is passed through a SM_FileHandle member 
// in the input parameter.
// All the members of the SM_FileHandle are initialized here.
// We store the file pointer in the fHandle->mgmtInfo member to use it in other functions.
// The name of the file to be initialized and the structure member are passed as input parameters to the function
extern RC openPageFile(char* file_name, SM_FileHandle *fHandle)
{

FILE *f;
int rc=0;
// The file is opened in r+ mode to enable writing to the file
f = fopen(file_name,"r+");
// File pointer value of NULL indicate that the fopen operation was unsuccessful
if(f==NULL)
        {
        //printf("Could not open the file");
	//If the fopen operation is unsuccessful we return appropriate return code as defined in dberror.h
        return RC_FILE_NOT_FOUND;
        }
else
        {
	// This section of code is executed if the fopen operation was successful
	//The fileName field is initialized with the name of the file
        fHandle->fileName = file_name;
	// The file is created with one page,hence we initialize totalNumPages value to 1
        fHandle->totalNumPages=1;
	// The current page position is 0 because the pages are counted as 0,1,2...
        fHandle->curPagePos =0;
	// The file pointer value is stored in the mgmtInfo field, this is helpful to access the file in other functions
        fHandle->mgmtInfo = (void*)f;
	// the status RC_OK indicates that all the statements executed as expected. 
        return RC_OK;
        }
        fclose(f);
}

//We close the file using the SM_FileHandle structure
// We use the file handling function fclose() to close the file
// The file pointer stored in fHandle->mgmtInfo is useful here to pass appropriate argument
extern RC closePageFile(SM_FileHandle *fHandle)
{
// The value of the file pointer stored in mgmtInfo field of SM_FileHandle structure 
fclose((FILE*)fHandle->mgmtInfo);
// returns RC_OK if the fclose operation is successfully complete.
return RC_OK;
}


//This function is to deallocate the memory allocated for the file.
//We use remove function to do so
extern RC destroyPageFile(char* filename){
int rem;
//remove function deallocates the memory allocated for the file
rem=remove(filename);
//remove function returns a value 0 if the memory is successfully deallocated
if(rem==0)
        {
	//  printf("file removed");
	// The function returns RC_OK if the memory is successfully deallocated
        return RC_OK;
        }
else
        {
  	//printf("File not found");
	// The memory deallocation is unsuccessfully if the file is non-existent
	// The RC_FILE_NOT_FOUND status is returned in such scenario
        return RC_FILE_NOT_FOUND;
        }
}


// getBlockPos method returns the current position of the page we are in.
extern int getBlockPos(SM_FileHandle *fHandle) {
	return (fHandle->curPagePos);
}

//blocksize = PAGE_SIZE as defined in dberror.h
//readBlock method returns the RC ok value if block read = 4096 bytes.
//reads number of pages requested
extern RC readBlock(int pageNum, SM_FileHandle *fHandle, SM_PageHandle memPage) {
	// retrieve the file using the file pointer value stored in the mgmtInfo field of SM_PageHandle member
	FILE *thisFile = fHandle->mgmtInfo; 
	//use the fseek function to set the read position on appropriate page location
	fseek(thisFile, pageNum*PAGE_SIZE, SEEK_SET); 
	// read 4096 bytes(PAGE_SIZE) of data from the desired location, we read one page of data using fread function
	size_t t = fread(memPage, 1, PAGE_SIZE, thisFile);
	if (t == PAGE_SIZE) {
		// if the fread function could successfully read complete page successfully return RC_OK
		return RC_OK;
	}
	else
		// if the fread function could not successfully read the page then return RC_READ_NON_EXISTING_PAGE
		return RC_READ_NON_EXISTING_PAGE;
}

//readFirstBlock method returns the RC ok value if the block read =  4096 bytes
extern RC readFirstBlock(SM_FileHandle *fHandle, SM_PageHandle memPage) {
	//read first block (ie when position = 0)
	// retrieve the file using the file pointer value stored in the mgmtInfo field of SM_PageHandle member
	FILE *thisFile = fHandle->mgmtInfo; 
	//set the file read position at 0 as we intend to read the first block.
	fseek(thisFile, 0, SEEK_SET); //retrieve the amount of data requested, starting at 0 //THIS IS THE 1/2 DIFF BETWEEN READFIRST N READBLOCK
	// read one page of data
	size_t t = fread(memPage, 1, PAGE_SIZE, thisFile);
	fHandle->curPagePos = 1;	//move to pos 1: 2/2 DIFF FROM READFIRST
	if (t == PAGE_SIZE) {
		// return RC_OK if we could successfully read the first page 
		return RC_OK;
	}
	else
		// if the file read operation is no successful then return RC_READ_NON_EXISTING_PAGE
		return RC_READ_NON_EXISTING_PAGE;
}

//readPreviousBlock
extern RC readPreviousBlock(SM_FileHandle *fHandle, SM_PageHandle memPage) {
	//read previous block (ie when position-=block)
	// retrieve the file using the file pointer value stored in the mgmtInfo field of SM_PageHandle member
	FILE *thisFile = fHandle->mgmtInfo; //retrieve file from fhandle
	//retrieve the current read position and subtract 1 to get the start position of the previous block
	int cminone = getBlockPos(fHandle) - 1;
	// set the read postion to that beginning of the previous block
	fseek(thisFile, cminone, SEEK_SET); 
	// read from the position set by fseek function
	size_t t = fread(memPage, 1, PAGE_SIZE, thisFile);
	if (t == PAGE_SIZE) {
		//if the read operation is successful then the value returned by fread would be equal to PAGE_SIZE
		// if the number of bytes read is equal to 
		return RC_OK;
	}
	else
		return RC_READ_NON_EXISTING_PAGE;

}


// This function writes a block of data into a file. The file info is passed through the input parameter SM_FileHandle
// The data to be written is given through input parameter memPage
extern RC writeBlock(int pageNum, SM_FileHandle *f, SM_PageHandle memPage)

{
int i=0, write_start=0;
size_t ret_code,t;
//      printf("In write block");
//      printf("mempage = %s", memPage);
// retrieve the file using the file pointer value stored in the mgmtInfo field of SM_PageHandle member
FILE *f_pointer = f->mgmtInfo;
	//set the write position to the required location according to the input parameter pageNum
        write_start=fseek(f_pointer, pageNum*4096,0);
	// Write a block 4096 bytes
        ret_code=fwrite(memPage, 1,4096, f_pointer);
//        printf("\n%zu", ret_code);

        if(ret_code == 4096)
                {
		// if the fwrite function could successfully write 4096 bytes to the file return RC_OK
                        return RC_OK;
                }
                else
                {
		// If the return code is not equal to 4096 then it implies the write operation was unsuccessful
		// return RC_WRITE_FAILED if the file write was unsuccessful.
                        return RC_WRITE_FAILED;
                }
}



// In this function we reuse the writeBlock function to write current block
extern RC writeCurrentBlock(SM_FileHandle *fHandle, SM_PageHandle memPage) {
	if (fHandle == NULL) {
		// If the fHandle member is null then it implies that the initialization the structure was not successful
		// Hence return RC_FILE_HANDLE_NOT_INIT as defined in db_error.h
		return RC_FILE_HANDLE_NOT_INIT;
	}
	else if (fHandle->curPagePos<0) {
		// if the pageposition is less than 0 then there is a problem with SM_PageHandle initialization 
		// and write operation failed
		return RC_WRITE_FAILED;
	}
	else {
		// if all the precondtions hold good invoke writeBlock to write the desired block
		return writeBlock(fHandle->curPagePos, fHandle, memPage);
	}
}

//writes an empty page to an existing file
// This function appends an empty block at the end of the file
extern RC appendEmptyBlock(SM_FileHandle *fHandle) {
	// checking the precondtion whether SM_FileHandle member is not null
	if (fHandle == NULL) {
		return RC_FILE_HANDLE_NOT_INIT;
	}
	else {
		// if the fHandle is a non-null member then retrieve the file pointer from mgmtInfo field
		FILE *f = fHandle->mgmtInfo;
		// set the file write location to the end of the file
		int s = fseek(fHandle->mgmtInfo, (fHandle->totalNumPages)*PAGE_SIZE, SEEK_SET);
		if (s != 0) {
			return RC_READ_NON_EXISTING_PAGE;
		}
		else {
			// Write an empty page at the end of the file
			int i = 0, w = 0;
			for (i = 0; i<PAGE_SIZE;i++) {
			// initialize the new page with NULL values.
				w = fwrite("\0", 1, 1, f);
			}
			//printf("Appended %d empty bytes.", w);
			fHandle->totalNumPages--;
			fHandle->curPagePos = fHandle->totalNumPages;
			// checking whether the empty page was successfully written
			if (w != PAGE_SIZE) {
				return RC_WRITE_FAILED;
			}
			else {
				return RC_OK;
			}
		}
	}
}


extern RC readCurrentBlock(SM_FileHandle *fHandle, SM_PageHandle memPage) {
	//Read the current
	int currentPage;
	//we can reach to the current position by the attribute  curPagePos which is //described to be count of pages from the beginning of the file.
	currentPage = fHandle->curPagePos;
	return readBlock(currentPage, fHandle, memPage);

}
extern RC readNextBlock(SM_FileHandle *fHandle, SM_PageHandle memPage) {
	//Read the previous
	//we can reach to the next position by increment the attribute  curPagePos .
	int nextPage;
	nextPage = fHandle->curPagePos + 1;
	return readBlock(nextPage, fHandle, memPage);


}
extern RC readLastBlock(SM_FileHandle *fHandle, SM_PageHandle memPage) {
	//Read next page relative to the curPagePos of the file.
	//The curPagePos should be moved to the page that was read.
	int lastPage;
	lastPage = fHandle->totalNumPages - 1;
	//If the user tries to read a block before the first page of after the last page of the file
	//f(fHandle->curPagePos>=fHandle->totalNumPages)
	//if yes  should return RC_READ_NON_EXISTING_PAGE.
	//return RC_READ_NON_EXISTING_PAGE
	return readBlock(lastPage, fHandle, memPage);

}
extern RC ensureCapacity(int numberOfPages, SM_FileHandle *fHandle) {
	int i,filepages;
	int increasepages;
	filepages = fHandle->totalNumPages;
	if (fHandle != NULL) {
		//check if the file has less than numberOfPages pages
		if (filepages<numberOfPages) {
			//yes increase the size to numberOfPages
			increasepages = numberOfPages - filepages;

			for (i = 0; i<increasepages; i++) {
				//Increase the number of pages in the file by one by using append empty block
				appendEmptyBlock(fHandle);
			}
		}
	}
	//else{
	//printf("the number of pages can not expand\n");}}
	return RC_OK;
	//else{
	//printf("the problem is the fHandle is null\n");}}
}
