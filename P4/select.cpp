#include "catalog.h"
#include "query.h"
#include "index.h"
#include <string>
#include <cstring>

/*
 * Selects records from the specified relation.
 *
 * Returns:
 * 	OK on success
 * 	an error code otherwise
 */
Status Operators::Select(const string & result,      // name of the output relation
	                 const int projCnt,          // number of attributes in the projection
		         const attrInfo projNames[], // the list of projection attributes
		         const attrInfo *attr,       // attribute used inthe selection predicate 
		         const Operator op,         // predicate operation
		         const void *attrValue)     // literal value in the predicate
{
    Status status;
    string relation = projNames[0].relName;
    AttrDesc attrProj[projCnt];
    AttrDesc attrs;
    int length = 0;
    
    for(int i = 0; i < projCnt; i++)
    {
        status = attrCat->getInfo(relation, projNames[i].attrName, attrs);
        if(status != OK)
            return status;
        attrProj[i] = attrs;
        length += attrs.attrLen;
    }
    
    if(attr != NULL)
    {
        status = attrCat->getInfo(relation, attr->attrName, attrs);
        if(status != OK)
            return status;
        if(op == EQ && attrs.indexed == true)
        {
            status = IndexSelect(result, projCnt, attrProj, &attrs, op, attrValue, length);
            if(status != OK)
                return status;
        }
        else
        {
            status = ScanSelect(result, projCnt, attrProj, &attrs, op, attrValue, length);
            if(status != OK)
                return status;
        }
    }
    else
    {   
        status = ScanSelect(result, projCnt, attrProj, NULL, op, attrValue, length);
        if(status != OK)
            return status;
    }
    return OK;
}

