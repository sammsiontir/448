#include "catalog.h"
#include "query.h"
#include "index.h"
#include <string>
#include <cstring>
#include <stdlib.h>

/* 
 * A simple scan select using a heap file scan
 */

Status Operators::ScanSelect(const string& result,       // Name of the output relation
                             const int projCnt,          // Number of attributes in the projection
                             const AttrDesc projNames[], // Projection list (as AttrDesc)
                             const AttrDesc* attrDesc,   // Attribute in the selection predicate
                             const Operator op,          // Predicate operator
                             const void* attrValue,      // Pointer to the literal value in the predicate
                             const int reclen)           // Length of a tuple in the result relation
{
    cout << "Algorithm: File Scan" << endl;
    
    Status status;
    string relation = projNames[0].relName;
    HeapFileScan* heapFileScan;

    if(attrDesc != NULL)
    {   
        Datatype d = static_cast<Datatype>(attrDesc->attrType);
        heapFileScan = new HeapFileScan(relation, attrDesc->attrOffset, attrDesc->attrLen, d, (char*)attrValue, op, status);
        if(status != OK)
            return status;
    }
    else
    {
        heapFileScan = new HeapFileScan(relation, status);
        if(status != OK)
            return status;
    }

    struct Record record;
    struct Record outTuple;
    outTuple.length = reclen;
    outTuple.data = malloc(outTuple.length);

    struct RID rid;
    HeapFile outFile = HeapFile(result, status);
    if(status != OK)
        return status;
    while(1)
    {
        status = heapFileScan->scanNext(rid, record);
        if(status != OK && status != FILEEOF)
            return status;
        if(status == FILEEOF)//determin when is the end of scan
            break;
        
        int offset = 0;
        for(int i = 0; i < projCnt; i++)
        {
            memcpy(outTuple.data + offset, record.data + projNames[i].attrOffset, projNames[i].attrLen);
            offset += projNames[i].attrLen;
        }
        
        status = outFile.insertRecord(outTuple, rid);
        if(status != OK)
            return status;
    }
    free(outTuple.data);
    delete heapFileScan;
    return OK;
}
