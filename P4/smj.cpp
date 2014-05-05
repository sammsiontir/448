#include "catalog.h"
#include "query.h"
#include "sort.h"
#include "index.h"
#include <string>
#include <cstring>
#include <stdlib.h>

/* Consider using Operators::matchRec() defined in join.cpp
 * to compare records when joining the relations */
  
Status Operators::SMJ(const string& result,           // Output relation name
                      const int projCnt,              // Number of attributes in the projection
                      const AttrDesc attrDescArray[], // Projection list (as AttrDesc)
                      const AttrDesc& attrDesc1,      // The left attribute in the join predicate
                      const Operator op,              // Predicate operator
                      const AttrDesc& attrDesc2,      // The left attribute in the join predicate
                      const int reclen)               // The length of a tuple in the result relation
{
    cout << "Algorithm: SM Join" << endl;
    
    // Create two SortedFile objects
    Status status;
    string relation = attrDesc1.relName;
    Datatype d = static_cast<Datatype>(attrDesc1.attrType);
    int size = GetMaxItems(attrDesc1);
    SortedFile sortedFile1 = SortedFile(relation, attrDesc1.attrOffset, attrDesc1.attrLen, d, size, status);
    if(status != OK)
        return status;
    
    relation = attrDesc2.relName;
    d = static_cast<Datatype>(attrDesc2.attrType);
    size = GetMaxItems(attrDesc2);
    SortedFile sortedFile2 = SortedFile(relation, attrDesc2.attrOffset, attrDesc2.attrLen, d, size, status);
    if(status != OK)
        return status;
    
    // Create records to record temporary tuples and output tuple
    struct Record record1;
    struct Record record1Old;
    struct Record record2;
    struct Record outTuple;
    outTuple.length = reclen;
    outTuple.data = malloc(outTuple.length);
    
    struct RID rid;
    HeapFile outFile = HeapFile(result, status);
    if(status != OK)
        return status;
    
    // Point to the first one tuple for two sorted files
    Status status1;
    Status status2;
    status1 = sortedFile1.next(record1);
    if(status1 != OK && status1 != FILEEOF)
        return status1;
    status2 = sortedFile2.next(record2);
    if(status2 != OK && status2 != FILEEOF)
        return status2;
    if(status1 == FILEEOF || status2 == FILEEOF)
        return FILEEOF;
    
    // Iteratively point to the next tuple in file1
    // until the tuple in file1 is smaller than or equal to that in file2
    while(1)
    {
        if(matchRec(record1, record2, attrDesc1, attrDesc2) < 0)
        {
            status1 = sortedFile1.next(record1);
            if(status1 != OK && status1 != FILEEOF)
                return status1;
            if(status1 == FILEEOF)
                return OK;
        }
        else
            break;
    }
    
    // If the two tuples pointed have the same value
    // set mark for both of them
    if(matchRec(record1, record2, attrDesc1, attrDesc2) == 0 )
    {
        sortedFile1.setMark();
    }
    sortedFile2.setMark();
    sortedFile2.gotoMark();

    while(1)
    {
        // Iteratively pointed to next tuple in file2
        // until tuples in file1 and file2 are different, quit from the loop
        while(1)
        {
            if(status2 != FILEEOF)
                status2 = sortedFile2.next(record2);
            if(status2 != OK && status2 != FILEEOF)
                return status2;
            if(status2 == FILEEOF)
                break;
            
            // Insert matched tuples into heapfile
            bool matchFlag = false;
            if(matchRec(record1, record2, attrDesc1, attrDesc2) == 0 )
                matchFlag = true;
            if(matchFlag == true)
            {
                int offset = 0;
                for(int i = 0; i < projCnt; i++)
                {
                    if(strcmp(attrDescArray[i].relName, attrDesc1.relName) == 0)
                    {
                        memcpy(outTuple.data + offset, record1.data + attrDescArray[i].attrOffset, attrDescArray[i].attrLen);
                        offset += attrDescArray[i].attrLen;
                    }
                    else if(strcmp(attrDescArray[i].relName, attrDesc2.relName) == 0)
                    {
                        memcpy(outTuple.data + offset, record2.data + attrDescArray[i].attrOffset, attrDescArray[i].attrLen);
                        offset += attrDescArray[i].attrLen;
                    }
                    else
                        cout<<"error!!!!!!!!!!"<<endl;
                }
                status = outFile.insertRecord(outTuple, rid);
                if(status != OK)
                    return status;
            }
            else
                break;
        }
        if(status1 != FILEEOF)
        {
            // Iteratively point to the next tuple in file1
            // until the tuple in file1 is smaller than or equal to that in file2
            while(1)
            {
                if(matchRec(record1, record2, attrDesc1, attrDesc2) < 0)
                {
                    record1Old = record1;
                    status1 = sortedFile1.next(record1);
                    if(status1 != OK && status1 != FILEEOF)
                        return status1;
                    if(status1 == FILEEOF)
                        return OK;
                }
                else
                    break;
            }
        }
        if(status1 == FILEEOF && status2 == FILEEOF)
            break;
        if(status1 != FILEEOF)
        {
            // If there are duplicates in file1
            // go to mark in file2
            if(matchRec(record1, record1Old, attrDesc1, attrDesc1) == 0)
            {
                sortedFile2.gotoMark();
                status2 = OK;
            }
            // If the tuples in file1 and file2 are the same
            // set mark for both files
            else if(matchRec(record1, record2, attrDesc1, attrDesc2) == 0 )
            {
                sortedFile1.setMark();
                sortedFile2.setMark();
                sortedFile2.gotoMark();
            }
        }
        else
            record1 = record1Old;
    }

    return OK;
}

int Operators::GetMaxItems(const AttrDesc& attrDesc)
{
    string relation = attrDesc.relName;
    int k = 800 * int(bufMgr->numUnpinnedPages());
    int attrCnt;
    AttrDesc* attrs;
    attrCat->getRelInfo(relation, attrCnt, attrs);
    int tupleLen = 0;
    for(int i = 0; i < attrCnt; i++)
    {
        tupleLen += attrs[i].attrLen;
    }
    int size = k / tupleLen;
    return size;
}

