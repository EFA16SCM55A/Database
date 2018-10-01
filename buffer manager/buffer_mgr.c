#include "storage_mgr.h"
#include "dberror.h"
#include "buffer_mgr_stat.h"
#include "buffer_mgr.h"

// convenience macros
#define MAKE_POOL()					\
		((BM_BufferPool *) malloc (sizeof(BM_BufferPool)))

#define MAKE_PAGE_HANDLE()				\
		((BM_PageHandle *) malloc (sizeof(BM_PageHandle)))

long long counter=0;
// Buffer Manager Interface Pool Handling
RC initBufferPool(BM_BufferPool *const bm, const char *const pageFileName, 
		const int numPages, ReplacementStrategy strategy,
		void *stratData){
	int i;
	BM_MgmtData *mgm = (BM_MgmtData*)malloc(sizeof(BM_MgmtData));
	SM_FileHandle *fh = (SM_FileHandle*)malloc(sizeof(SM_FileHandle));
	PageFrame *buffer = (PageFrame*)malloc(numPages*sizeof(PageFrame));

	mgm->pages = buffer;
	mgm->fhandle = fh;

	bm->mgmtData = (void*)mgm;
	bm->pageFile = pageFileName;
	bm->numPages = numPages;
	bm->strategy = strategy;
	bm->numReadIO = 0;
	bm->numWriteIO = 0;

	openPageFile(bm->pageFile,fh);

        for(i=0;i<numPages;i++) {
		mgm->pages[i].data=(SM_PageHandle)(malloc(sizeof(char*)*4096));
		mgm->pages[i].pageNum=-1;
		mgm->pages[i].dirty_bit=0;
		mgm->pages[i].fix_count=0;
		printf("\n1: %s \n2: %d \n3: %d \n4: %d",
		    buffer[i].data,buffer[i].pageNum,
		    buffer[i].dirty_bit, buffer[i].fix_count);
	}
	printf("Management data initialized\n");
	return RC_OK;
}

void printBufferPool(BM_BufferPool *const bm)
{
	PageFrame *buffer = ((BM_MgmtData*)(bm->mgmtData))->pages;
	int i=0;
	for(i=0; i<bm->numPages; i++) {
		printf("Frame[%d] : PageNumber = %d, AgeCount = %lld,"
		    "FixCount = %d,DirtyBit = %d, Data = %s\n",
		    i, buffer[i].pageNum,buffer[i].ageBit,
		    buffer[i].fix_count, buffer[i].dirty_bit,
		    buffer[i].data);
	}
}

RC shutdownBufferPool(BM_BufferPool *const bm) {
	int i,num = bm->numPages,rc=0;
        int start = 0;
        PageFrame *buffer = ((BM_MgmtData*)(bm->mgmtData))->pages;
	BM_MgmtData *mgmtData = (BM_MgmtData*)(bm->mgmtData);
        for(i=0;i<num;i++) {
                printf("\n1: %s \n2: %d \n3: %d \n4: %d",
		    buffer[i].data,buffer[i].pageNum, buffer[i].dirty_bit, buffer[i].fix_count);
                if(buffer[i].fix_count==0 && buffer[i].dirty_bit==0)
                {
                        printf("\nThe frame %d need not be written back to the disk", i);
                }
                else
                if(buffer[i].fix_count!=0){
                        printf("\n The page %d is still in use", buffer[i].pageNum);
                }
                if(buffer[i].dirty_bit==1){
                        printf("\nThe page %d  has to be written back to the disk\n",i);
			
                       rc =  writeBlock(buffer[i].pageNum,mgmtData->fhandle,buffer[i].data);
			if(rc==RC_OK){
			bm->numWriteIO++;
                        printf("\n page successfully written to disk");
			}
			else
			return rc;
                }
        }
        free(buffer);
	return RC_OK;
}
RC forceFlushPool(BM_BufferPool *const bm) {
	int i,num = bm->numPages;
	int rc=0;
	PageFrame *buffer = ((BM_MgmtData*)(bm->mgmtData))->pages;
	BM_MgmtData *mgmtData = (BM_MgmtData*)(bm->mgmtData);

	for (i=0; i<num; i++) {
		 if(buffer[i].dirty_bit==1 && buffer[i].fix_count==0) {
        	                printf("\nThe page %d  has to be written back to the disk\n",i);
        	                rc = writeBlock(buffer[i].pageNum,mgmtData->fhandle,buffer[i].data);
				if (rc == RC_OK) {
					buffer[i].dirty_bit = 0;
	        	                printf("\n page successfully written to disk");
                                	bm->numWriteIO++;
				}
		}
	}
	return RC_OK;
}

// Buffer Manager Interface Access Pages
RC markDirty(BM_BufferPool *const bm, BM_PageHandle *const page) {
	int num = bm->numPages;
	PageFrame *buffer = ((BM_MgmtData*)(bm->mgmtData))->pages;
	int j=0;
	//because i is number of pages go through them
	for(j=0; j<num; j++) {
		// check if the page number in the buffer to update it is the same page in memory
		// because i is number of pages go through them
		//check if the page number in the buffer to update it is the same page in memory
		if(buffer[j].pageNum == page->pageNum) {
			// mark it dirty by set the dirty bit to one as indicator that page is used to write
			buffer[j].dirty_bit=1;
			return RC_OK;
		}
	}
	return RC_OK;
}

RC unpinPage(BM_BufferPool *const bm, BM_PageHandle *const page){
	int i,num = bm->numPages;
	PageFrame *buffer = ((BM_MgmtData*)(bm->mgmtData))->pages;
	printf("Unpinning page number %d\n", page->pageNum);
	//check if the page number in the buffer to unpin it is the same page in memory
	int j=0;
	printBufferPool(bm);
	//because i is number of pages go through them
	for(j=0; j<num; j++) {
		if((buffer[j].pageNum == page->pageNum) && (buffer[j].fix_count>0)) {
		    // unpin the page by setting the fix count to zero
			buffer[j].fix_count--;
			printf("Unpinned page number %d\n", page->pageNum);
			return RC_OK;
		}
	}
	printBufferPool(bm);
	return RC_OK;
}

RC forcePage(BM_BufferPool *const bm, BM_PageHandle *const page) {
	int i=0,rc=0,num = bm->numPages;
	PageFrame *buffer = ((BM_MgmtData*)(bm->mgmtData))->pages;
	BM_MgmtData *mgmtData = (BM_MgmtData*)(bm->mgmtData);
	int j=0;
	int pageFound=0;
	//because i is number of pages go through them
	for(j=0; j<bm->numPages; j++) {
		if(buffer[j].pageNum == page->pageNum) {
			pageFound=1;
			if (buffer[j].dirty_bit == 1) {
				// write the block
				rc = writeBlock(buffer[j].pageNum,mgmtData->fhandle,buffer[j].data);
				if (rc == RC_OK) {
					//set the dirty bit to zero
					buffer[j].dirty_bit= 0;
                                	bm->numWriteIO++;
					return RC_OK;
				} else {
					printf("Writeblock failed in forcePage rc = %d\n", rc);
					return rc;
				}
			} else {
				printf("forcePage: dirty_bit for pagenumber %d is not set\n", page->pageNum);
				return RC_OK;
			}
		}
			
	}
	if (pageFound == 0) {
		printf("forcePage:: Pagenumber %d, NOT FOUND !\n",page->pageNum);
	}
	return RC_OK;
}

RC pinPage(BM_BufferPool *const bm, BM_PageHandle *const page, const PageNumber pageNum) {
	PageFrame *buffer = ((BM_MgmtData*)(bm->mgmtData))->pages;
	BM_MgmtData *mgmtData = (BM_MgmtData*)(bm->mgmtData);
	int i,pnum,rc;
	long long leastused;
	int positiontoreplace, max_age_bit;

	BM_PageHandle replacePage;
	counter++;
	

	printBufferPool(bm);
	// first iteration to search if the page exists in the buffer
	for(i=0;i<bm->numPages;i++) {
		if(buffer[i].pageNum == pageNum) {
			page->data = buffer[i].data;
			page->pageNum = pageNum;
			buffer[i].fix_count++;
			buffer[i].ageBit=counter;
			printf("Returning from pinpage i = %d, pageNume = %d (1st condition)\n",i, pageNum);
	printBufferPool(bm);
			return RC_OK;
		}	
	}

	// second iteration to check whether there is an empty frame available
	// in the buffer to bring in the required page
	for(i=0;i<bm->numPages;i++) {
		if(buffer[i].pageNum==-1) {
 			rc = readBlock(pageNum, mgmtData->fhandle,(SM_PageHandle)(buffer[i].data));
			if(rc == RC_OK) {
				printf("\nread page from file into the buffer\n");
				bm->numReadIO++;
				buffer[i].pageNum=pageNum;
				buffer[i].fix_count=1;
				buffer[i].ageBit = counter;
				page->data = buffer[i].data;
				page->pageNum = pageNum;

				return RC_OK;
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
						printf("ERROR Return Code = %d\n", rc);
						return rc;
					}
				} else {
					printf("ERROR Return Code = %d\n", rc);
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
		printf("In page replacement, incoming page number = %d\n", pageNum);
		leastused=mgmtData->pages[0].ageBit;
		pnum=0;
		for (i=0; i < bm->numPages; i++) {
			if ((mgmtData->pages[i].ageBit < leastused) && (mgmtData->pages[i].fix_count == 0)) {
				leastused=mgmtData->pages[i].ageBit;
				pnum=i;
			}
        	}

		printf("Evicting frame %d, pagenumber %d\n", pnum, buffer[pnum].pageNum);

		replacePage.pageNum = buffer[pnum].pageNum;
		replacePage.data = buffer[pnum].data;
	
		
		// the page number to be replaced is in pnum
		rc = forcePage(bm, &replacePage);
		if (rc != RC_OK) {
			printf("ForcePage returned with err code - %d\n", rc);	
		}

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


int getNumReadIO (BM_BufferPool *const bm){
        return bm->numReadIO;
}

int getNumWriteIO (BM_BufferPool *const bm){
        return bm->numWriteIO;
}
