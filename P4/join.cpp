#include "catalog.h"
#include "query.h"
#include "sort.h"
#include "index.h"
#include <cmath>
#include <cstring>

#define MAX(a,b) ((a) > (b) ? (a) : (b))
#define DOUBLEERROR 1e-07

/*
 * Joins two relations
 *
 * Returns:
 * 	OK on success
 * 	an error code otherwise
 */

Status Operators::Join(const string& result,           // Name of the output relation 
                       const int projCnt,              // Number of attributes in the projection
    	               const attrInfo projNames[],     // List of projection attributes
    	               const attrInfo* attr1,          // Left attr in the join predicate
    	               const Operator op,              // Predicate operator
    	               const attrInfo* attr2)          // Right attr in the join predicate
{
    Status status;
    string relation;
    AttrDesc attrProj[projCnt];
    AttrDesc attrs;
    int length = 0;
    
    for(int i = 0; i < projCnt; i++)
    {
        relation = projNames[i].relName;
        status = attrCat->getInfo(relation, projNames[i].attrName, attrs);
        if(status != OK)
            return status;
        attrProj[i] = attrs;
        length += attrs.attrLen;
    }
    
    AttrDesc attrs1;
    relation = attr1->relName;
    status = attrCat->getInfo(relation, attr1->attrName, attrs1);
    if(status != OK)
        return status;
    
    AttrDesc attrs2;
    relation = attr2->relName;
    status = attrCat->getInfo(relation, attr2->attrName, attrs2);
    if(status != OK)
        return status;

    if(strcmp(attrs1.attrName, attrs2.attrName) == 0 && op == EQ)
    {
        if(attrs1.indexed == true || attrs2.indexed == true)
        {
            status = INL(result, projCnt, attrProj, attrs1, op, attrs2, length);
            if(status != OK)
                return status;
        }
        else
        {
            status = SMJ(result, projCnt, attrProj, attrs1, op, attrs2, length);
            if(status != OK)
                return status;
        }
    }
    else
    {
        status = SNL(result, projCnt, attrProj, attrs1, op, attrs2, length);
        if(status != OK)
            return status;
    }
	return OK;
}

// Function to compare two record based on the predicate. Returns 0 if the two attributes 
// are equal, a negative number if the left (attrDesc1) attribute is less that the right 
// attribute, otherwise this function returns a positive number.
int Operators::matchRec(const Record& outerRec,     // Left record
                        const Record& innerRec,     // Right record
                        const AttrDesc& attrDesc1,  // Left attribute in the predicate
                        const AttrDesc& attrDesc2)  // Right attribute in the predicate
{
    int tmpInt1, tmpInt2;
    double tmpFloat1, tmpFloat2, floatDiff;

    // Compare the attribute values using memcpy to avoid byte alignment issues
    switch(attrDesc1.attrType)
    {
        case INTEGER:
            memcpy(&tmpInt1, (char *) outerRec.data + attrDesc1.attrOffset, sizeof(int));
            memcpy(&tmpInt2, (char *) innerRec.data + attrDesc2.attrOffset, sizeof(int));
            return tmpInt1 - tmpInt2;

        case DOUBLE:
            memcpy(&tmpFloat1, (char *) outerRec.data + attrDesc1.attrOffset, sizeof(double));
            memcpy(&tmpFloat2, (char *) innerRec.data + attrDesc2.attrOffset, sizeof(double));
            floatDiff = tmpFloat1 - tmpFloat2;
            return (fabs(floatDiff) < DOUBLEERROR) ? 0 : floatDiff;

        case STRING:
            return strncmp(
                (char *) outerRec.data + attrDesc1.attrOffset, 
                (char *) innerRec.data + attrDesc2.attrOffset, 
                MAX(attrDesc1.attrLen, attrDesc2.attrLen));
    }

    return 0;
}