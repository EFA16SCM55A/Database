#include "storage_mgr.h"
#include "dberror.h"
#include "buffer_mgr_stat.h"
#include "buffer_mgr.h"

// convenience macros
#define MAKE_POOL()					\
		((BM_BufferPool *) malloc (sizeof(BM_BufferPool)))

#define MAKE_PAGE_HANDLE()				\
		((BM_PageHandle *) malloc (sizeof(BM_PageHandle)))


// We use this counter to calculate the age of a page in the bufferpool. 
// We set the AgeBit of pages in the bufferpool and use it during page replacement.
long long counter=0;
// Buffer Manager Interface Pool Handling
// Initializing the bufferpool, we create a bufferpool corresponding to a single file
// This function initializes the bufferpool for certain file passed as input parameter.
RC initBufferPool(BM_BufferPool *const bm, const char *const pageFileName, 
		const int numPages, ReplacementStrategy strategy,
		void *stratData){
	int i;
	// The management info field in the BM_BufferPool structure is a member of BM_BufferPool
	// It holds reference to the buffer pool and other statistics information
	BM_MgmtData *mgm = (BM_MgmtData*)malloc(sizeof(BM_MgmtData));
	// A buffer pool corresponds to a file on disk, you store the file handle in this structure
	SM_FileHandle *fh = (SM_FileHandle*)malloc(sizeof(SM_FileHandle));
	// A buffer pool is made up of multiple page frames, here is the structure of each page frame.
	PageFrame *buffer = (PageFrame*)malloc(numPages*sizeof(PageFrame));
	// initializing all the structure members
	mgm->pages = buffer;
	mgm->fhandle = fh;

	bm->mgmtData = (void*)mgm;
	bm->pageFile = pageFileName;
	bm->numPages = numPages;
	bm->strategy = strategy;
	bm->numReadIO = 0;
	bm->numWriteIO = 0;
	// open the file for which the bufferpool is created
	openPageFile(bm->pageFile,fh);
	// initialize members of the structure PageFrame for each page in the buffer pool
        for(i=0;i<numPages;i++) {
		mgm->pages[i].data=(SM_PageHandle)(malloc(sizeof(char*)*PAGE_SIZE));
		mgm->pages[i].pageNum=-1;
		mgm->pages[i].dirty_bit=0;
		mgm->pages[i].fix_count=0;
	}
	return RC_OK;
}


// This function shuts down the bufferpool by freeing resources.
// It writes all the modified pages(dirty pages) onto the disk
// and frees the resources used by the buffer manager
RC shutdownBufferPool(BM_BufferPool *const bm) {
	int i,num = bm->numPages,rc=0;
        int start = 0;
	// The pages are stored in the mgmt info field of BM_BufferPool structure 
	// as an array of PageFrame type.
        PageFrame *buffer = ((BM_MgmtData*)(bm->mgmtData))->pages;
	BM_MgmtData *mgmtData = (BM_MgmtData*)(bm->mgmtData);
	// The changes made in the buffer have to be written back to the disk.
	// The dirty bit signifies the changes made, in this loop we write all the 
	// modified pages onto the disk.
        for(i=0;i<num;i++) {
                if(buffer[i].dirty_bit==1){
			
                       rc =  writeBlock(buffer[i].pageNum,mgmtData->fhandle,buffer[i].data);
			if(rc==RC_OK){
			// numWriteIO member of BM_BufferPool holds information about 
			// number of writes performed onto the disk.
			bm->numWriteIO++;
			}
			else
			return rc;
                }
        }
//        free(buffer);
	return RC_OK;
}


// forceFlushPool writes all the dirty pages in the buffer onto the disk
RC forceFlushPool(BM_BufferPool *const bm) {
	int i,num = bm->numPages;
	int rc=0;
	PageFrame *buffer = ((BM_MgmtData*)(bm->mgmtData))->pages;
	BM_MgmtData *mgmtData = (BM_MgmtData*)(bm->mgmtData);
	// The if condition checks whether certain page is modified in the memory
	// and whether any client is accessing the page
	// If the page is modified and no client is using the page then it is written to disk.
	for (i=0; i<num; i++) {
		 if(buffer[i].dirty_bit==1 && buffer[i].fix_count==0) {
        	                rc = writeBlock(buffer[i].pageNum,mgmtData->fhandle,buffer[i].data);
				if (rc == RC_OK) {
					buffer[i].dirty_bit = 0;
                                	bm->numWriteIO++;
				}
		}
	}
	return RC_OK;
}



// Buffer Manager Interface Access Pages
// A page frame is marked dirty when it is modified in the buffer or memory.
// We associate a dirty_bit which each page in the buffer.
// The dirty bit value is set to 1 when the page is modified by a client requesting the page
RC markDirty(BM_BufferPool *const bm, BM_PageHandle *const page) {
	int num = bm->numPages;
	PageFrame *buffer = ((BM_MgmtData*)(bm->mgmtData))->pages;
	int j=0;
	//Traverse through all the pages in the buffer pool
	for(j=0; j<num; j++) {
		if(buffer[j].pageNum == page->pageNum) {
			// mark it dirty by set the dirty bit to one as indicator that page is used to write
			buffer[j].dirty_bit=1;
			return RC_OK;
		}
	}
	return RC_OK;
}

// A page is pinned into the bufferpool when a client requests for it, the fix_count field of page frame
// indicates the number of users accessing the page.
// When the client do not need the page any more, the user has to unpin the page. Unpinning the page in turn 
// decrements the fix_count of the page frame
RC unpinPage(BM_BufferPool *const bm, BM_PageHandle *const page){
	int i,num = bm->numPages;
	PageFrame *buffer = ((BM_MgmtData*)(bm->mgmtData))->pages;
	int j=0;
	//because i is number of pages go through them
	for(j=0; j<num; j++) {
		if((buffer[j].pageNum == page->pageNum) && (buffer[j].fix_count>0)) {
		    // unpin the page by decrementing the fix count
			buffer[j].fix_count--;
			return RC_OK;
		}
	}
	return RC_OK;
}


// forcePage function writes a page onto the disk if it is modified in the memory
RC forcePage(BM_BufferPool *const bm, BM_PageHandle *const page) {
	int i=0,rc=0,num = bm->numPages;
	PageFrame *buffer = ((BM_MgmtData*)(bm->mgmtData))->pages;
	BM_MgmtData *mgmtData = (BM_MgmtData*)(bm->mgmtData);
	int j=0;
	int pageFound=0;
	// The for loop here is locating the required page in the buffer pool 
	for(j=0; j<bm->numPages; j++) {
		if(buffer[j].pageNum == page->pageNum) {
			pageFound=1;
			if (buffer[j].dirty_bit == 1) {
				// Once you find the page and it is modified in the buffer pool 
				// You write it to the disk.
				rc = writeBlock(buffer[j].pageNum,mgmtData->fhandle,buffer[j].data);
				if (rc == RC_OK) {
					//Reset the dirty bit once the updates are written onto the disk
					buffer[j].dirty_bit= 0;
                                	bm->numWriteIO++;
					return RC_OK;
				} else {
					return rc;
				}
			} else {
				return RC_OK;
			}
		}
			
	}
	if (pageFound == 0) {
	}
	return RC_OK;
}


// pinPage function is used to bring in or provide access to a page on a client request.
// There are three possiblities here 
// 1. The page is already present in the buffer pool
// 2. The page is not in buffer pool but there is an empty page frame in the buffer pool
//	we can read the page into the empty slot
// 3. The buffer pool is full and the page is not in the buffer pool. In such case we have to
// 	use a page replacement algorithm FIFO/LRU to identify and replace a page in the buffer pool
//	to bring in new page.
RC pinPage(BM_BufferPool *const bm, BM_PageHandle *const page, const PageNumber pageNum) {
	PageFrame *buffer = ((BM_MgmtData*)(bm->mgmtData))->pages;
	BM_MgmtData *mgmtData = (BM_MgmtData*)(bm->mgmtData);
	int i,pnum,rc;
	long long leastused;
	int positiontoreplace, max_age_bit;

	BM_PageHandle replacePage;
	// This is a global counter for the buffer pool to keep track of the age of a page frame 
	counter++;
	
	// first iteration to search if the page exists in the buffer,if the page exists, then increment
	// the fix count  and set the ageBit to current counter value. This will be used in LRU.
	for(i=0;i<bm->numPages;i++) {
		if(buffer[i].pageNum == pageNum) {
			page->data = buffer[i].data;
			page->pageNum = pageNum;
			buffer[i].fix_count++;
			if (bm->strategy == RS_LRU)
				buffer[i].ageBit=counter;
			return RC_OK;
		}	
	}

	// second iteration to check whether there is an empty frame available
	// in the buffer to bring in the required page
	for(i=0;i<bm->numPages;i++) {
		if(buffer[i].pageNum==-1) {
 			rc = readBlock(pageNum, mgmtData->fhandle,(SM_PageHandle)(buffer[i].data));
			if(rc == RC_OK) {
				bm->numReadIO++;
				buffer[i].pageNum=pageNum;
				buffer[i].fix_count=1;
				buffer[i].ageBit = counter;
				page->data = buffer[i].data;
				page->pageNum = pageNum;

				return RC_OK;
				// If the user requests for a non-existent page number in file we extend the 
				// file size and bring in required page into the buffer.
			} else if (rc == RC_READ_NON_EXISTING_PAGE) {
				rc = ensureCapacity(pageNum+1,mgmtData->fhandle);
				if (rc == RC_OK) {
 					rc = readBlock(pageNum, mgmtData->fhandle,(SM_PageHandle)(buffer[i].data));
					if (rc == RC_OK) {
						bm->numReadIO++;
						buffer[i].pageNum=pageNum;
						buffer[i].fix_count=1;
						buffer[i].ageBit = counter;
						page->data = buffer[i].data;
						page->pageNum = pageNum;
						return RC_OK;
					} else {
						return rc;
					}
				} else {
					return rc;
				}
			}

		}
	}
	// If the page is not available in the buffer and there is no empty slot
	// to bring in the new page we use a page replacement algorithm
	if ((bm->strategy == RS_LRU) || (bm->strategy == RS_FIFO)) {

		// find the page with minimum value of agebit and retrieve its pageNum
		// check the fixcount and dirty bit values of the page and replace the page
		leastused=mgmtData->pages[0].ageBit;
		pnum=0;
		for (i=0; i < bm->numPages; i++) {
			if ((mgmtData->pages[i].ageBit < leastused) && (mgmtData->pages[i].fix_count == 0)) {
				leastused=mgmtData->pages[i].ageBit;
				pnum=i;
			}
        	}


		replacePage.pageNum = buffer[pnum].pageNum;
		replacePage.data = buffer[pnum].data;
	
		
		// the page number to be replaced is in pnum. The page data has to be written to the disk
		// invoke force page to write the page data into the disk
		rc = forcePage(bm, &replacePage);

		rc = readBlock(pageNum, mgmtData->fhandle,(SM_PageHandle)(buffer[pnum].data));
		if (rc == RC_OK) {
			bm->numReadIO++;
			buffer[pnum].pageNum = pageNum;
			buffer[pnum].dirty_bit=0;
			buffer[pnum].fix_count=1;
			buffer[pnum].ageBit=counter;
		        page->data = buffer[pnum].data;
		        page->pageNum = pageNum;
			return RC_OK;
		} else if (rc == RC_READ_NON_EXISTING_PAGE) {
			rc = ensureCapacity(pageNum+1, mgmtData->fhandle);
			if (rc == RC_OK) {
				rc = readBlock(pageNum, mgmtData->fhandle,(SM_PageHandle)(buffer[pnum].data));
		        	if (rc == RC_OK) {
					bm->numReadIO++;
					buffer[pnum].pageNum = pageNum;
					buffer[pnum].dirty_bit=0;
					buffer[pnum].fix_count=1;
					buffer[pnum].ageBit=counter;
		        		page->data = buffer[pnum].data;
		        		page->pageNum = pageNum;
					return RC_OK;
				} else {
		        	        return rc;
		        	}
			} else {
				return rc;
			}
		} else {
			return rc;
		}
	}
	return rc;
}

// Statistics Interface
// function returns an array of PageNumbers (of size numPages) where the ith element
// is the number of the page stored in the ith page frame.
// An empty page frame is represented using the constant NO_PAGE. 
PageNumber *getFrameContents (BM_BufferPool *const bm) {
	PageNumber *arr;
	arr = (PageNumber*)(malloc(sizeof(PageNumber)*bm->numPages));
	int i, num = bm->numPages;

	for(i=0;i<num;i++) {     //traverse all pages
		if((((BM_MgmtData*)(bm->mgmtData))->pages[i]).pageNum==-1) {
			arr[i]=NO_PAGE;//need to declare this constant?
		} else {
        		arr[i]=(((BM_MgmtData*)(bm->mgmtData))->pages[i]).pageNum;
		}
	}
	return arr;
}


// The function returns an array which holds the dirty bits of the page frames in the buffer pool
bool *getDirtyFlags (BM_BufferPool *const bm){
        int i, num = bm->numPages;
        bool *arr = (bool*)malloc(sizeof(bool)*bm->numPages); //initialize array
        for(i=0;i<num;i++){     //traverse all pages
                if( (((BM_MgmtData*)(bm->mgmtData))->pages[i]).dirty_bit ==1){ //check if page[i] is dirty //mgmtdata=pages and fhandle 
                    arr[i]=TRUE;
                }
                else arr[i]=FALSE;
            }
    return arr;
}

int *getFixCounts (BM_BufferPool *const bm){
        //allocate space for return array
         int *fixCountArr = (int*)malloc(bm->numPages * sizeof(int));
         int i;
        //unpack mgmtData which contains fix FixCounts
         PageFrame *buffer = ((BM_MgmtData*)bm->mgmtData)->pages;
        //iterate through all pages and put fix count data in array
          for (i=0; i<bm->numPages; i++){
                 fixCountArr[i] = buffer[i].fix_count;
                  }
  return fixCountArr;
}


// returns the number of reads performed in the buffer pool
int getNumReadIO (BM_BufferPool *const bm){
        return bm->numReadIO;
}



//returns the number of writes performed in the buffer pool
int getNumWriteIO (BM_BufferPool *const bm){
        return bm->numWriteIO;
}
