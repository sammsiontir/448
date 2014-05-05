#include "catalog.h"
#include "query.h"
#include "index.h"
#include <string.h>
#include <stdlib.h>

/*
 * Inserts a record into the specified relation
 *
 * Returns:
 * 	OK on success
 * 	an error code otherwise
 */

Status Updates::Insert(const string& relation,      // Name of the relation
                       const int attrCnt,           // Number of attributes specified in INSERT statement
                       const attrInfo attrList[])   // Value of attributes specified in INSERT statement
{
    /* Your solution goes here */
    Status status;
    AttrDesc* attrs;
    int relAttrCount;
    status = attrCat->getRelInfo(relation, relAttrCount, attrs);
    if(status != OK)
        return status;
    if(relAttrCount != attrCnt)
        return ATTRTYPEMISMATCH;
    
    int offset[attrCnt];
    int attrPosition[attrCnt];
    int attrLen[attrCnt];
    bool indexFlag[attrCnt];
    
    for(int i = 0; i < relAttrCount; i++)
    {
        for(int j = 0; j<attrCnt; j++)
        {
            if(strcmp(attrList[j].attrName, attrs[i].attrName) == 0)
            {
                //attrList[j].attrLen = attrs[i].attrLen;
                attrLen[i] = attrs[i].attrLen;
                offset[i] = attrs[i].attrOffset;
                attrPosition[i] = j;
                if(attrs[i].indexed == 1)
                    indexFlag[i] = true;
                else
                    indexFlag[i] = false;
                break;
            }
        }
    }
    
    struct Record record;
    record.length = attrs[relAttrCount-1].attrOffset + attrs[relAttrCount-1].attrLen;
    record.data = malloc(record.length);
    free(attrs);
    
    for(int i = 0; i < attrCnt; i++)
    {
        memcpy((record.data + offset[i]), attrList[attrPosition[i]].attrValue, attrLen[i]);
    }
    
    HeapFile heapFile = HeapFile(relation, status);
    if(status != OK)
        return status;
    struct RID rid;
    status = heapFile.insertRecord(record, rid);
    if(status != OK)
        return status;
    
    for(int i = 0; i < attrCnt; i++)
    {
        if(indexFlag[i] == true)
        {
            Datatype d = static_cast<Datatype>(attrList[attrPosition[i]].attrType);
            Index index = Index(relation, attrs[i].attrOffset, attrLen[i], d, 0, status); //not sure how to determine unique..(0)
            if(status != OK)
                return status;
            status = index.insertEntry(attrList[attrPosition[i]].attrValue, rid);
            if(status != OK)
                return status;
        }
    }
    
    free(record.data);
    return OK;
}
