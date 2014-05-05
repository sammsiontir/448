#include "catalog.h"
#include "query.h"
#include "index.h"
#include <string>
#include <cstring>
#include <stdlib.h>

Status Operators::IndexSelect(const string& result,       // Name of the output relation
                              const int projCnt,          // Number of attributes in the projection
                              const AttrDesc projNames[], // Projection list (as AttrDesc)
                              const AttrDesc* attrDesc,   // Attribute in the selection predicate
                              const Operator op,          // Predicate operator
                              const void* attrValue,      // Pointer to the literal value in the predicate
                              const int reclen)           // Length of a tuple in the output relation
{
    cout << "Algorithm: Index Select" << endl;

    Status status;
    string relation = projNames[0].relName;
    
    Datatype d = static_cast<Datatype>(attrDesc->attrType);
    Index index = Index(relation, attrDesc->attrOffset, attrDesc->attrLen, d, 0, status);
    if(status != OK)
        return status;
    HeapFileScan heapFileScan = HeapFileScan(relation, status);
    if(status != OK)
        return status;
    
    struct Record record;
    struct Record outTuple;
    outTuple.length = reclen;
    outTuple.data = malloc(outTuple.length);
    
    struct RID rid;
    HeapFile outFile = HeapFile(result, status);
    if(status != OK)
        return status;
    status = index.startScan(attrValue);
    if(status != OK)
        return status;
    while(1)
    {
        status = index.scanNext(rid);
        if(status != OK && status != NOMORERECS)
            return status;
        if(status == NOMORERECS)//determin when is the end of scan
            break;

        status = heapFileScan.getRandomRecord(rid, record);
        if(status != OK)
            return status;

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
    status = index.endScan();
    if(status != OK)
        return status;
    free(outTuple.data);
    return OK;
}

