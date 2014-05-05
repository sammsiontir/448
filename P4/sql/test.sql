-- Test selection queries
-- scan select
SELECT * FROM DA WHERE DA.serial = 11;
SELECT * FROM DA WHERE DA.dkey > 100.0;
SELECT * FROM DA WHERE DA.dkey < 90.0;
SELECT * FROM DA WHERE DA.dkey >= 106.7;
SELECT * FROM DA WHERE DA.filler = '00062 string record';

SELECT DA.serial FROM DA WHERE DA.serial = 11;
SELECT DA.serial, DA.iKey FROM DA WHERE DA.serial = 11;
SELECT DA.iKey FROM DA WHERE DA.serial = 11;
SELECT DA.iKey, DA.serial FROM DA WHERE DA.serial = 11;
SELECT DA.serial, DA.iKey, DA.filler FROM DA WHERE DA.serial = 11;

-- index select
CREATE INDEX DA (serial);
SELECT * FROM DA WHERE DA.serial = 11;
SELECT * FROM DA WHERE DA.dkey > 100.0;
SELECT * FROM DA WHERE DA.dkey < 90.0;
SELECT * FROM DA WHERE DA.dkey >= 106.7;
SELECT * FROM DA WHERE DA.filler = '00062 string record';

SELECT DA.serial FROM DA WHERE DA.serial = 11;
SELECT DA.serial, DA.iKey FROM DA WHERE DA.serial = 11;
SELECT DA.iKey FROM DA WHERE DA.serial = 11;
SELECT DA.iKey, DA.serial FROM DA WHERE DA.serial = 11;
SELECT DA.serial, DA.iKey, DA.filler FROM DA WHERE DA.serial = 11;
DROP INDEX DA (serial);



-- Test Join queries
-- use INL
-- with 1 index
CREATE INDEX DA (ikey);
SELECT * FROM DA, DB WHERE DA.ikey = DB.ikey; 
-- with 2 index
CREATE INDEX DB (ikey);
SELECT * FROM DA, DB WHERE DA.ikey = DB.ikey; 
DROP INDEX DA (ikey);
DROP INDEX DB (ikey);

-- use SMJ
SELECT * FROM DA, DB WHERE DA.ikey = DB.ikey; 
SELECT DA.serial, DA.ikey, DB.ikey, DB.serial FROM DA, DB WHERE DA. serial = DA.serial;

-- use SNL
-- with no index
SELECT DA.ikey, DB.serial FROM DA, DB WHERE DA.ikey < DB.serial; 
-- with 1 index
CREATE INDEX DA (ikey);
SELECT DA.ikey, DB.serial FROM DA, DB WHERE DA.ikey < DB.serial; 
DROP INDEX DA (ikey);

CREATE INDEX DB (serial);
SELECT DA.ikey, DB.serial FROM DA, DB WHERE DA.ikey < DB.serial; 
DROP INDEX DB (serial);
-- with 2 index
CREATE INDEX DA (ikey);
CREATE INDEX DB (serial);
SELECT DA.ikey, DB.serial FROM DA, DB WHERE DA.ikey < DB.serial; 
DROP INDEX DA (ikey);
DROP INDEX DB (serial);
