#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "record_mgr.h"
#include "buffer_mgr.h"
#include "storage_mgr.h"
#define A_SIZE 15

// This is custom data structure defined for making the use of Record Manager.
typedef struct ManagementData
{
	BM_PageHandle pageHandle;	// Buffer Manager PageHandle 
	// Buffer Manager's Buffer Pool for using Buffer Manager	
	BM_BufferPool bufferPool;
	RID recordID;
	Expr *condition;//for scan functions
	int tuplesCount;
	int freePage;// stores the first free page available for the buffer
	int scanCount;//holds the count of records scanned
} ManagementData;


ManagementData *mgmtDatax;

// This function initializes the Record Manager
extern RC initRecordManager (void *mgmtData)
{
	initStorageManager();
	return RC_OK;
}

// This functions shuts down the Record Manager by deallocating the resources
extern RC shutdownRecordManager ()
{
	mgmtDatax = NULL;
	free(mgmtDatax);
	return RC_OK;
}

// createSchema function creates a new schema
extern Schema *createSchema (int numAttr, char **attrNames, DataType *dataTypes, int *typeLength, int keySize, int *keys)
{
        // Allocate memory space to schema
        Schema *schema = (Schema *) malloc(sizeof(Schema));
	// assigning values to schema struct members with incoming parameters
        schema->numAttr = numAttr;
        schema->attrNames = attrNames;
        schema->dataTypes = dataTypes;
        schema->typeLength = typeLength;
        schema->keySize = keySize;
        schema->keyAttrs = keys;

        return schema;
}

// This function removes a schema from memory and de-allocates all the memory space allocated to the schema.
extern RC freeSchema (Schema *schema)
{
        // De-allocating memory space occupied by 'schema'
        free(schema);
        return RC_OK;
}


// createTable function creates a TABLE with table name "name" having schema specified by "schema"
extern RC createTable (char *name, Schema *schema)
{
	// Allocating memory space for the managementData structure
	mgmtDatax = (ManagementData*) malloc(sizeof(ManagementData));
	createPageFile(name);

	// Initalizing the Buffer Pool to create 100 pages, the replacement policy is Least Recently Used
	initBufferPool(&mgmtDatax->bufferPool, name, 100, RS_LRU, NULL);
	// create initial page, this will be page 0 of the bufferpool, it has all the management info for the table we store
	char data[PAGE_SIZE];
	char *pageOffset = data;
	 
	int result, k;
	// In the first page we store the table info in the order:number of tuples,pointer to free page,number of attributes and schema information
	// Setting number of tuples to 0
	*(int*)pageOffset = 0; 

	// Incrementing the page pointer to next int location
	pageOffset = pageOffset + sizeof(int);
	
	// Setting first page to 1 as next free page.New rows can be stored in page number 1
	*(int*)pageOffset = 1;

	// Advancing the offset to store next piece of info
	pageOffset = pageOffset + sizeof(int);

	// Storing the info of number of attributes according to schema used when we scan the record
	*(int*)pageOffset = schema->numAttr;

	// Advancing the offset to store the next piece of table info
	pageOffset = pageOffset + sizeof(int); 

	// Storing the keysize value in the first page
	*(int*)pageOffset = schema->keySize;

	// Incrementing pointer by sizeof(int) because Key Size of attributes is an integer
	pageOffset = pageOffset + sizeof(int);
	// Allocating space for a record prototype
	for(k = 0; k < schema->numAttr; k++)
    	{
		// Setting attribute name
       		strncpy(pageOffset, schema->attrNames[k], A_SIZE);
	       	pageOffset = pageOffset + A_SIZE;
	
		// Setting data type of attribute
	       	*(int*)pageOffset = (int)schema->dataTypes[k];

		// Incrementing pointer by sizeof(int) because we have data type using integer constants
	       	pageOffset = pageOffset + sizeof(int);

		// Setting length of datatype of the attribute
	       	*(int*)pageOffset = (int) schema->typeLength[k];

		// Incrementing pointer by sizeof(int) because type length is an integer
	       	pageOffset = pageOffset + sizeof(int);
    	}

	SM_FileHandle fileHandle;
	// using functions from buffer manager and storage manager to initialize the pages and return appropriate return code
	if((result = createPageFile(name)) != RC_OK)
		return result;
	if((result = openPageFile(name, &fileHandle)) != RC_OK)
		return result;
	if((result = writeBlock(0, &fileHandle, data)) != RC_OK)
		return result;
	if((result = closePageFile(&fileHandle)) != RC_OK)
		return result;
	return RC_OK;
}


// openTable function opens the table with table name which is passed as an argument
extern RC openTable (RM_TableData *rel, char *name)
{
	SM_PageHandle pageHandle;    
	
	int attributeCount, k;
	
	// Setting table's management data to the mgmtData field in RM_TableData structure
	rel->mgmtData = mgmtDatax;
	// Setting the table's name
	rel->name = name;
    
	// Pinning a page i.e. putting a page in Buffer Pool using Buffer Manager
	pinPage(&mgmtDatax->bufferPool, &mgmtDatax->pageHandle, 0);
	//  Read the required information from page number 0, this data was stored in it while creating the table
	// Setting the initial pointer (0th location) if the record manager's page data
	pageHandle = (char*) mgmtDatax->pageHandle.data;
	 // Retrieving number of tuples 
        mgmtDatax->tuplesCount= *(int*)pageHandle;
        pageHandle = pageHandle + sizeof(int);

	
	// Retrieving the pagenumber of first free page where we can store the rows
	mgmtDatax->freePage= *(int*) pageHandle;
    	pageHandle = pageHandle + sizeof(int);
	

	// Retrieving number of attributes from the page file
    	attributeCount = *(int*)pageHandle;
	pageHandle = pageHandle + sizeof(int);
 	
	Schema *schema;

	// Allocating memory space to 'schema'
	schema = (Schema*) malloc(sizeof(Schema));
    
	// Setting schema's parameters
	schema->numAttr = attributeCount;
	schema->attrNames = (char**) malloc(sizeof(char*) *attributeCount);
	schema->dataTypes = (DataType*) malloc(sizeof(DataType) *attributeCount);
	schema->typeLength = (int*) malloc(sizeof(int) *attributeCount);

      
	for(k = 0; k < schema->numAttr; k++)
    	{
		// Setting attribute name
		schema->attrNames[k]= (char*) malloc(A_SIZE);
		strncpy(schema->attrNames[k], pageHandle, A_SIZE);
		pageHandle = pageHandle + A_SIZE;
	   
		// Setting data type of attribute
		schema->dataTypes[k]= *(int*) pageHandle;
		pageHandle = pageHandle + sizeof(int);

		// Setting length of datatype (length of STRING) of the attribute
		schema->typeLength[k]= *(int*)pageHandle;
		pageHandle = pageHandle + sizeof(int);
	}
	// For persistence of schema information we store the  newly created schema to the table's schema
	rel->schema = schema;	

	// Unpinning the page, removes the page from buffer pool
	unpinPage(&mgmtDatax->bufferPool, &mgmtDatax->pageHandle);

	// the changes made in the bufferpool are written back to the disk
	forcePage(&mgmtDatax->bufferPool, &mgmtDatax->pageHandle);

	return RC_OK;
}   
//closeTable deallocates the memory allocated to table
extern RC closeTable (RM_TableData *rel)
{
	// Storing the Table's meta data
	ManagementData *mgmtDatax = rel->mgmtData;
	
	// Shutting down Buffer Pool	
	shutdownBufferPool(&mgmtDatax->bufferPool);
	return RC_OK;
}


// delete table function removes the table in this case the pages created to store table data are removed.
extern RC deleteTable (char *name)
{
	// removing the pages allocated for the table in the memory
	destroyPageFile(name);
	return RC_OK;
}

// This function returns the number of tuples stored in structure RM_TableData
extern int getNumTuples (RM_TableData *rel)
{
	// The value of the number of tuples is stored in ManagementData structure, we return that value here
	ManagementData *mgmtDatax = rel->mgmtData;
	return mgmtDatax->tuplesCount;
}


// creates a record to store a tuple
extern RC createRecord (Record **record, Schema *schema)
{
        // Allocate memory for record structure
        Record *newRecord = (Record*) malloc(sizeof(Record));
        // Calculate the record size
        int recordSize = getRecordSize(schema);
        // Allocate  memory  for data of new record    
        newRecord->data= (char*) malloc(recordSize);
        // Setting page and slot position. -1 to indicate it is an empty record and we can write data into it
        newRecord->id.page = newRecord->id.slot = -1;
        // Getting the starting position in memory of the record's data
        char *dataPointer = newRecord->data;
        // '-' is used for Tombstone mechanism. We set it to '-' because the record is empty.
        *dataPointer = '-';
        // Append '\0' which means NULL in C to the record after tombstone. ++ because we need to move the position by one before adding NULL
        *(++dataPointer) = '\0';
        // Set the newly created record to 'record' which passed as argument
        *record = newRecord;
        return RC_OK;

}

// insertRecord function inserts a record into an empty slot in the page file 

extern RC insertRecord(RM_TableData *rel, Record *record)
{
	// Retrieving table meta data stored in the table
	ManagementData *mgmtDatax = rel->mgmtData;	
	
	// Setting the Record ID for this record
	RID *recordID = &record->id; 
	
	char *data, *slotPointer;
	
	// Getting the size of record in bytes 
	int recordSize = getRecordSize(rel->schema);
	
	// Setting first free page to the current page
	recordID->page = mgmtDatax->freePage;
//	printf(" free page %d", recordID->page);
	// pinPage function brings in required page into main memory
	pinPage(&mgmtDatax->bufferPool, &mgmtDatax->pageHandle, recordID->page);
	
	// Setting the data to initial position of record's data
	data = mgmtDatax->pageHandle.data;
	
	// Getting a free slot using our custom function
	recordID->slot = findFreeSlot(data, recordSize);

	while(recordID->slot == -1)
	{
		// If the pinned page doesn't have a free slot then unpin that page
		unpinPage(&mgmtDatax->bufferPool, &mgmtDatax->pageHandle);	
		
		// Incrementing page
		recordID->page++;
		
		// Bring the new page into the BUffer Pool using Buffer Manager
		pinPage(&mgmtDatax->bufferPool, &mgmtDatax->pageHandle, recordID->page);
		
		// Setting the data to initial position of record's data		
		data = mgmtDatax->pageHandle.data;

		// Again checking for a free slot using our custom function
		recordID->slot = findFreeSlot(data, recordSize);
	}
	
	slotPointer = data;
	
	// Mark page dirty as it was modified
	markDirty(&mgmtDatax->bufferPool, &mgmtDatax->pageHandle);
	
	// Calculating slot start position
	slotPointer = slotPointer + (recordID->slot * recordSize);

	// Appending '+' as tombstone '+' denotes the slot is full
	*slotPointer = '+';

	// Copy the record's data to the memory location pointed by slotPointer
	memcpy(++slotPointer, record->data + 1, recordSize - 1);

	// remove the page from the BUffer Pool
	unpinPage(&mgmtDatax->bufferPool, &mgmtDatax->pageHandle);
	
	// Incrementing count of tuples
	mgmtDatax->tuplesCount++;
	
	// Pinback the page	
	pinPage(&mgmtDatax->bufferPool, &mgmtDatax->pageHandle, 0);
	//printf("\npinPage here %d\n",recordID->page);
	return RC_OK;
}


// This function deletes a record having Record ID "id" in the table referenced by "rel"
extern RC deleteRecord (RM_TableData *rel, RID id)
{
	// Retrieving our meta data stored in the table
	ManagementData *mgmtDatax = rel->mgmtData;
	
	// Pinning the page which has the record which we want to update
	pinPage(&mgmtDatax->bufferPool, &mgmtDatax->pageHandle, id.page);

	// Update free page because this page has a free location where we can store a record 
	mgmtDatax->freePage = id.page;
	
	char *data = mgmtDatax->pageHandle.data;

	// Calculate the record size
	int recordSize = getRecordSize(rel->schema);
	// insertRecord function inserts a record into the page file 
	// Setting data pointer to the specific slot of the record
	data = data + (id.slot * recordSize);
	
	// '-' is used for Tombstone mechanism. '-' deontes the slot is empty
	*data = '-';
		
	// Mark the page dirty because it has been modified
	markDirty(&mgmtDatax->bufferPool, &mgmtDatax->pageHandle);

	// Unpin the page after the record is retrieved since the page is no longer required to be in memory
	unpinPage(&mgmtDatax->bufferPool, &mgmtDatax->pageHandle);

	return RC_OK;
}


// This function updates a record referenced by "record" in the table referenced by "rel"
extern RC updateRecord (RM_TableData *rel, Record *record)
{	
	// Retrieving table metadata from the MgmtData structure
	ManagementData *mgmtDatax = rel->mgmtData;
	
	// bring the page which has the desired record into memory
	pinPage(&mgmtDatax->bufferPool, &mgmtDatax->pageHandle, record->id.page);

	char *data;

	// Getting the size of the record
	int recordSize = getRecordSize(rel->schema);

	// Set the Record's ID
	RID id = record->id;

	// Getting record data's memory location and calculating the start position of the new data
	data = mgmtDatax->pageHandle.data;
	data = data + (id.slot * recordSize);
	
	// '+' is used for Tombstone mechanism. It denotes that the record is not empty
	*data = '+';
	
	// Copy the new record data to the exisitng record, the update will overwrite the previous record
	memcpy(++data, record->data + 1, recordSize - 1 );
	
	// Mark the page dirty because it has been modified
	markDirty(&mgmtDatax->bufferPool, &mgmtDatax->pageHandle);

	// Unpin the page after the record is retrieved since the page is no longer required to be in memory
	unpinPage(&mgmtDatax->bufferPool, &mgmtDatax->pageHandle);
	
	return RC_OK;	
}

// This function retrieves a record having Record ID "id" in the table referenced by "rel".
// The result record is stored in the location referenced by "record"
extern RC getRecord (RM_TableData *rel, RID id, Record *record)
{
	// Retrieving our meta data stored in the table
	ManagementData *mgmtDatax = rel->mgmtData;
	
	// Pinning the page which has the record we want to retreive
	pinPage(&mgmtDatax->bufferPool, &mgmtDatax->pageHandle, id.page);
	// Getting the size of the record
	int recordSize = getRecordSize(rel->schema);
	char *dataPointer = mgmtDatax->pageHandle.data;
	dataPointer = dataPointer + (id.slot * recordSize);
	
	if(*dataPointer != '+')
	{
		// This status signifies that the record you were looking for is not present in the table
		return RC_RM_NO_TUPLE_WITH_GIVEN_RID;
	}
	else
	{
		// Setting the Record ID
		record->id = id;

		// Setting the pointer to data field of 'record' so that we can copy the data of the record
		char *data = record->data;

		// Copy the new record into the given memory location
		memcpy(++data, dataPointer + 1, recordSize - 1);
	}

	// Unpin the page after the record is retrieved since the page is no longer required to be in memory
	unpinPage(&mgmtDatax->bufferPool, &mgmtDatax->pageHandle);

	return RC_OK;
}


// This function scans all the records using the condition
extern RC startScan (RM_TableData *rel, RM_ScanHandle *scan, Expr *cond)
{
	// Checking if scan condition (test expression) is present
	if (cond == NULL)
	{
		return RC_SCAN_CONDITION_NOT_FOUND;
	}

	// Open the table in memory
	openTable(rel, "ScanTable");

    	ManagementData *scanManager;
	ManagementData *tableManager;

	// Allocating some memory to the scanManager
    	scanManager = (ManagementData*) malloc(sizeof(ManagementData));
    	
	// Setting the scan's meta data to our meta data
    	scan->mgmtData = scanManager;
    	
	// 1 to start scan from the first page
    	scanManager->recordID.page = 1;
    	
	// 0 to start scan from the first slot	
	scanManager->recordID.slot = 0;
	
	// 0 because this just initializing the scan. No records have been scanned yet    	
	scanManager->scanCount = 0;

	// Setting the scan condition
    	scanManager->condition = cond;
    	
	// Setting the our meta data to the table's meta data
    	tableManager = rel->mgmtData;

	// Setting the tuple count
    	tableManager->tuplesCount = A_SIZE;

	// Setting the scan's table i.e. the table which has to be scanned using the specified condition
    	scan->rel= rel;

	return RC_OK;
}


// This function scans each record in the table and stores the result record (record satisfying the condition)
// in the location pointed by  'record'.
extern RC next (RM_ScanHandle *scan, Record *record)
{
	// Initiliazing scan data
	ManagementData *scanManager = scan->mgmtData;
	ManagementData *tableManager = scan->rel->mgmtData;
    	Schema *schema = scan->rel->schema;
	
	// Checking if scan condition (test expression) is present
	if (scanManager->condition == NULL)
	{
		return RC_SCAN_CONDITION_NOT_FOUND;
	}

	Value *result = (Value *) malloc(sizeof(Value));
   
	char *data;
   	
	// Getting record size of the schema
	int recordSize = getRecordSize(schema);

	// Calculating Total number of slots
	int totalSlots = PAGE_SIZE / recordSize;

	// Getting Scan Count
	int scanCount = scanManager->scanCount;

	// Getting tuples count of the table
	int tuplesCount = tableManager->tuplesCount;

	// Checking if the table contains tuples. If the tables doesn't have tuple, then return respective message code
	if (tuplesCount == 0)
		return RC_RM_NO_MORE_TUPLES;

	// Iterate through the tuples
	while(scanCount <= tuplesCount)
	{  
		// If all the tuples have been scanned, execute this block
		if (scanCount <= 0)
		{
			// printf("INSIDE If scanCount <= 0 \n");
			// Set PAGE and SLOT to first position
			scanManager->recordID.page = 1;
			scanManager->recordID.slot = 0;
		}
		else
		{
			// printf("INSIDE Else scanCount <= 0 \n");
			scanManager->recordID.slot++;

			// If all the slots have been scanned execute this block
			if(scanManager->recordID.slot >= totalSlots)
			{
				scanManager->recordID.slot = 0;
				scanManager->recordID.page++;
			}
		}

		// Pinning the page i.e. putting the page in buffer pool
		pinPage(&tableManager->bufferPool, &scanManager->pageHandle, scanManager->recordID.page);
			
		// Retrieving the data of the page			
		data = scanManager->pageHandle.data;

		// Calulate the data location from record's slot and record size
		data = data + (scanManager->recordID.slot * recordSize);
		
		// Set the record's slot and page to scan manager's slot and page
		record->id.page = scanManager->recordID.page;
		record->id.slot = scanManager->recordID.slot;

		// Intialize the record data's first location
		char *dataPointer = record->data;

		// '-' is used for Tombstone mechanism.
		*dataPointer = '-';
		
		memcpy(++dataPointer, data + 1, recordSize - 1);

		// Increment scan count because we have scanned one record
		scanManager->scanCount++;
		scanCount++;

		// Test the record for the specified condition (test expression)
		evalExpr(record, schema, scanManager->condition, &result); 

		// v.boolV is TRUE if the record satisfies the condition
		if(result->v.boolV == TRUE)
		{
			// Unpin the page i.e. remove it from the buffer pool.
			unpinPage(&tableManager->bufferPool, &scanManager->pageHandle);
			// Return SUCCESS			
			return RC_OK;
		}
	}
	
	// Unpin the page i.e. remove it from the buffer pool.
	unpinPage(&tableManager->bufferPool, &scanManager->pageHandle);
	
	// Reset the Scan Manager's values
	scanManager->recordID.page = 1;
	scanManager->recordID.slot = 0;
	scanManager->scanCount = 0;
	
	// None of the tuple satisfy the condition and there are no more tuples to scan
	return RC_RM_NO_MORE_TUPLES;
}


// This function closes the scan operation.
extern RC closeScan (RM_ScanHandle *scan)
{
	ManagementData *scanManager = scan->mgmtData;
	ManagementData *mgmtDatax = scan->rel->mgmtData;

	// Check if scan was incomplete
	if(scanManager->scanCount > 0)
	{
		// Unpin the page i.e. remove it from the buffer pool.
		unpinPage(&mgmtDatax->bufferPool, &mgmtDatax->pageHandle);
		
		// Reset the Scan Manager's values
		scanManager->scanCount = 0;
		scanManager->recordID.page = 1;
		scanManager->recordID.slot = 0;
	}
	
	// De-allocate all the memory space allocated to the scans's meta data (our custom structure)
    	scan->mgmtData = NULL;
    	free(scan->mgmtData);  
	
	return RC_OK;
}

// This function returns the record size of the schema referenced by "schema"
extern int getRecordSize (Schema *schema)
{
	int size = 0, i; // offset set to zero
	
	// Iterating through all the attributes in the schema
	for(i = 0; i < schema->numAttr; i++)
	{
		switch(schema->dataTypes[i])
		{
			// Switch depending on DATA TYPE of the ATTRIBUTE
			case DT_STRING:
				// If attribute is STRING then size = typeLength (Defined Length of STRING)
				size = size + schema->typeLength[i];
				break;
			case DT_INT:
				// If attribute is INTEGER, then add size of INT
				size = size + sizeof(int);
				break;
			case DT_FLOAT:
				// If attribite is FLOAT, then add size of FLOAT
				size = size + sizeof(float);
				break;
			case DT_BOOL:
				// If attribite is BOOLEAN, then add size of BOOLEAN
				size = size + sizeof(bool);
				break;
		}
	}
	return ++size;
}

// This function sets the offset (in bytes) from initial position to the specified attribute of the record into the 'result' parameter passed through the function
RC attrOffset (Schema *schema, int attrNum, int *result)
{
	int i;
	*result = 1;

	// Iterating through all the attributes in the schema
	for(i = 0; i < attrNum; i++)
	{
		// Switch depending on DATA TYPE of the ATTRIBUTE
		switch (schema->dataTypes[i])
		{
			// Switch depending on DATA TYPE of the ATTRIBUTE
			case DT_STRING:
				// If attribute is STRING then size = typeLength (Defined Length of STRING)
				*result = *result + schema->typeLength[i];
				break;
			case DT_INT:
				// If attribute is INTEGER, then add size of INT
				*result = *result + sizeof(int);
				break;
			case DT_FLOAT:
				// If attribite is FLOAT, then add size of FLOAT
				*result = *result + sizeof(float);
				break;
			case DT_BOOL:
				// If attribite is BOOLEAN, then add size of BOOLEAN
				*result = *result + sizeof(bool);
				break;
		}
	}
	return RC_OK;
}
// This function removes the record from the memory.
extern RC freeRecord (Record *record)
{
	// De-allocating memory space allocated to record and freeing up that space
	free(record);
	return RC_OK;
}

// This function retrieves an attribute from the given record in the specified schema
extern RC getAttr (Record *record, Schema *schema, int attrNum, Value **value)
{
	int offset = 0;

	// Getting the ofset value of attributes depending on the attribute number
	attrOffset(schema, attrNum, &offset);

	// Allocating memory space for the Value data structure where the attribute values will be stored
	Value *attribute = (Value*) malloc(sizeof(Value));

	// Getting the starting position of record's data in memory
	char *dataPointer = record->data;
	
	// Adding offset to the starting position
	dataPointer = dataPointer + offset;

	// If attrNum = 1
	schema->dataTypes[attrNum] = (attrNum == 1) ? 1 : schema->dataTypes[attrNum];
	
	// Retrieve attribute's value depending on attribute's data type
	switch(schema->dataTypes[attrNum])
	{
		case DT_STRING:
		{
     			// Getting attribute value from an attribute of type STRING
			int length = schema->typeLength[attrNum];
			// Allocate space for string hving size - 'length'
			attribute->v.stringV = (char *) malloc(length + 1);

			// Copying string to location pointed by dataPointer and appending '\0' which denotes end of string in C
			strncpy(attribute->v.stringV, dataPointer, length);
			attribute->v.stringV[length] = '\0';
			attribute->dt = DT_STRING;
      			break;
		}

		case DT_INT:
		{
			// Getting attribute value from an attribute of type INTEGER
			int value = 0;
			memcpy(&value, dataPointer, sizeof(int));
			attribute->v.intV = value;
			attribute->dt = DT_INT;
      			break;
		}
    
		case DT_FLOAT:
		{
			// Getting attribute value from an attribute of type FLOAT
	  		float value;
	  		memcpy(&value, dataPointer, sizeof(float));
	  		attribute->v.floatV = value;
			attribute->dt = DT_FLOAT;
			break;
		}

		case DT_BOOL:
		{
			// Getting attribute value from an attribute of type BOOLEAN
			bool value;
			memcpy(&value,dataPointer, sizeof(bool));
			attribute->v.boolV = value;
			attribute->dt = DT_BOOL;
      			break;
		}

		default:
			printf("Serializer not defined for the given datatype. \n");
			break;
	}

	*value = attribute;
	return RC_OK;
}


// This function sets the attribute value in the record in the specified schema
extern RC setAttr (Record *record, Schema *schema, int attrNum, Value *value)
{
	int offset = 0;

	// Getting the ofset value of attributes depending on the attribute number
	attrOffset(schema, attrNum, &offset);

	// Getting the starting position of record's data in memory
	char *dataPointer = record->data;
	
	// Adding offset to the starting position
	dataPointer = dataPointer + offset;
		
	switch(schema->dataTypes[attrNum])
	{
		case DT_STRING:
		{
			// Setting attribute value of an attribute of type STRING
			// Getting the legeth of the string as defined while creating the schema
			int length = schema->typeLength[attrNum];

			// Copying attribute's value to the location pointed by record's data (dataPointer)
			strncpy(dataPointer, value->v.stringV, length);
			dataPointer = dataPointer + schema->typeLength[attrNum];
		  	break;
		}

		case DT_INT:
		{
			// Setting attribute value of an attribute of type INTEGER
			*(int *) dataPointer = value->v.intV;	  
			dataPointer = dataPointer + sizeof(int);
		  	break;
		}
		
		case DT_FLOAT:
		{
			// Setting attribute value of an attribute of type FLOAT
			*(float *) dataPointer = value->v.floatV;
			dataPointer = dataPointer + sizeof(float);
			break;
		}
		
		case DT_BOOL:
		{
			// Setting attribute value of an attribute of type STRING
			*(bool *) dataPointer = value->v.boolV;
			dataPointer = dataPointer + sizeof(bool);
			break;
		}

		default:
			printf("Serializer not defined for the given datatype. \n");
			break;
	}			
	return RC_OK;
}


// This function returns a free slot within a page
int findFreeSlot(char *data, int recordSize)
{
        int i, totalSlots = PAGE_SIZE / recordSize;

        for (i = 0; i < totalSlots; i++)
                if (data[i * recordSize] != '+')
                        return i;
        return -1;
}

