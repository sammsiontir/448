#include "catalog.h"
#include "query.h"
#include "sort.h"
#include "index.h"
#include <string>
#include <cstring>
#include <stdlib.h>

Status Operators::SNL(const string& result,           // Output relation name
                      const int projCnt,              // Number of attributes in the projection
                      const AttrDesc attrDescArray[], // Projection list (as AttrDesc)
                      const AttrDesc& attrDesc1,      // The left attribute in the join predicate
                      const Operator op,              // Predicate operator
                      const AttrDesc& attrDesc2,      // The left attribute in the join predicate
                      const int reclen)               // The length of a tuple in the result relation
{
    cout << "Algorithm: Simple NL Join" << endl;

    Status status;
    string relation = attrDesc1.relName;
    HeapFileScan heapFileScanOuter = HeapFileScan(relation, status);
    if(status != OK)
        return status;

    struct Record recordScanOuter;
    struct Record recordScanInner;
    struct Record outTuple;
    outTuple.length = reclen;
    outTuple.data = malloc(outTuple.length);
    
    struct RID rid;
    HeapFile outFile = HeapFile(result, status);
    if(status != OK)
        return status;

    while(1)
    {
        status = heapFileScanOuter.scanNext(rid, recordScanOuter);
        if(status != OK && status != FILEEOF)
            return status;
        if(status == FILEEOF)//determin when is the end of scan
            break;
        
        relation = attrDesc2.relName;
        HeapFileScan* heapFileScanInner = new HeapFileScan(relation, status);
        if(status != OK)
            return status;
        while(1)
        {
            status = heapFileScanInner->scanNext(rid, recordScanInner);
            if(status != OK && status != FILEEOF)
                return status;
            if(status == FILEEOF)//determin when is the end of scan
                break;
            
            bool matchFlag = false;
            switch(op)
            {
                case LT:
                    if(matchRec(recordScanOuter, recordScanInner, attrDesc1, attrDesc2) < 0 )
                        matchFlag = true;
                    break;
                case LTE:
                    if(matchRec(recordScanOuter, recordScanInner, attrDesc1, attrDesc2) <= 0 )
                        matchFlag = true;
                    break;
                case EQ:
                    if(matchRec(recordScanOuter, recordScanInner, attrDesc1, attrDesc2) == 0 )
                        matchFlag = true;
                    break;
                case GTE:
                    if(matchRec(recordScanOuter, recordScanInner, attrDesc1, attrDesc2) >= 0 )
                        matchFlag = true;
                    break;
                case GT:
                    if(matchRec(recordScanOuter, recordScanInner, attrDesc1, attrDesc2) > 0 )
                        matchFlag = true;
                    break;
                case NE:
                    if(matchRec(recordScanOuter, recordScanInner, attrDesc1, attrDesc2) != 0 )
                        matchFlag = true;
                    break;
                case NOTSET:
                    break;
            }
            if(matchFlag == true)
            {
                int offset = 0;
                for(int i = 0; i < projCnt; i++)
                {
                    if(strcmp(attrDescArray[i].relName, attrDesc1.relName) == 0)
                    {
                        memcpy(outTuple.data + offset, recordScanOuter.data + attrDescArray[i].attrOffset, attrDescArray[i].attrLen);
                        offset += attrDescArray[i].attrLen;
                    }
                    else if(strcmp(attrDescArray[i].relName, attrDesc2.relName) == 0)
                    {
                        memcpy(outTuple.data + offset, recordScanInner.data + attrDescArray[i].attrOffset, attrDescArray[i].attrLen);
                        offset += attrDescArray[i].attrLen;
                    }
                    else
                        cout<<"error!!!!!!!!!!"<<endl;
                }
                status = outFile.insertRecord(outTuple, rid);
                if(status != OK)
                    return status;
            }
        }
        delete heapFileScanInner;
    }
    free(outTuple.data);
    return OK;
}

