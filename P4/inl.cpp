#include "catalog.h"
#include "query.h"
#include "sort.h"
#include "index.h"
#include <string>
#include <cstring>
#include <stdlib.h>

/* 
 * Indexed nested loop evaluates joins with an index on the 
 * inner/right relation (attrDesc2)
 */

Status Operators::INL(const string& result,           // Name of the output relation
                      const int projCnt,              // Number of attributes in the projection
                      const AttrDesc attrDescArray[], // The projection list (as AttrDesc)
                      const AttrDesc& attrDesc1,      // The left attribute in the join predicate
                      const Operator op,              // Predicate operator
                      const AttrDesc& attrDesc2,      // The left attribute in the join predicate
                      const int reclen)               // Length of a tuple in the output relation
{
    cout << "Algorithm: Indexed NL Join" << endl;
    
    Status status;
    AttrDesc attrIndexed;
    AttrDesc attrScan;
    if(attrDesc1.indexed == true)
    {
        attrIndexed = attrDesc1;
        attrScan = attrDesc2;
    }
    else
    {
        attrIndexed = attrDesc2;
        attrScan = attrDesc1;
    }
    //cout<<"aaa"<<endl;
    string relation = attrIndexed.relName;
    Datatype d = static_cast<Datatype>(attrIndexed.attrType);
    Index index = Index(relation, attrIndexed.attrOffset, attrIndexed.attrLen, d, 0, status);
    if(status != OK)
        return status;
    relation = attrScan.relName;
    HeapFileScan heapFileScan = HeapFileScan(relation, status);
    relation = attrIndexed.relName;
    HeapFileScan heapFileGetRecord = HeapFileScan(relation, status);
    if(status != OK)
        return status;
    //cout<<"bbb"<<endl;
    struct Record recordScan;
    struct Record recordIndexed;
    struct Record outTuple;
    outTuple.length = reclen;
    outTuple.data = malloc(outTuple.length);
    //cout<<"ccc"<<endl;
    struct RID rid;
    HeapFile outFile = HeapFile(result, status);
    if(status != OK)
        return status;
    
    while(1)
    {
        status = heapFileScan.scanNext(rid, recordScan);
        if(status != OK && status != FILEEOF)
            return status;
        if(status == FILEEOF)//determin when is the end of scan
            break;
        //cout<<"222"<<endl;
        void* attrValue = malloc(attrScan.attrLen);
        memcpy(attrValue, recordScan.data + attrScan.attrOffset, attrScan.attrLen);
        //cout<<"333"<<endl;
        status = index.startScan(attrValue);
        if(status != OK)
            return status;
        //cout<<"AAA"<<endl;
        while(1)
        {
            status = index.scanNext(rid);
            if(status != OK && status != NOMORERECS)
                return status;
            if(status == NOMORERECS)//determin when is the end of scan
                break;
            //cout<<"BBB"<<endl;
            status = heapFileGetRecord.getRandomRecord(rid, recordIndexed);
            if(status != OK)
                return status;
            //cout<<"CCC"<<endl;
            int offset = 0;
            for(int i = 0; i < projCnt; i++)
            {
                if(strcmp(attrDescArray[i].relName, attrScan.relName) == 0)
                {
                    memcpy(outTuple.data + offset, recordScan.data + attrDescArray[i].attrOffset, attrDescArray[i].attrLen);
                    offset += attrDescArray[i].attrLen;
                }
                else if(strcmp(attrDescArray[i].relName, attrIndexed.relName) == 0)
                {
                    memcpy(outTuple.data + offset, recordIndexed.data + attrDescArray[i].attrOffset, attrDescArray[i].attrLen);
                    offset += attrDescArray[i].attrLen;
                }
                else
                    cout<<"error!!!!!!!!!!"<<endl;
            }
            //cout<<"DDD"<<endl;
            status = outFile.insertRecord(outTuple, rid);
            if(status != OK)
                return status;
        }
        status = index.endScan();
        if(status != OK)
            return status;
        delete attrValue;
        //cout<<"EEE"<<endl;
    }
    free(outTuple.data);
    return OK;
}

