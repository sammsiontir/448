CREATE TABLE PUBLIC_USER_INFORMATION AS ( SELECT * FROM singhmk.PUBLIC_USER_INFORMATION);
CREATE TABLE PUBLIC_ARE_FRIENDS AS ( SELECT * FROM singhmk.PUBLIC_ARE_FRIENDS);
CREATE TABLE PUBLIC_PHOTO_INFORMATION AS ( SELECT * FROM singhmk.PUBLIC_PHOTO_INFORMATION);
CREATE TABLE PUBLIC_TAG_INFORMATION AS ( SELECT * FROM singhmk.PUBLIC_TAG_INFORMATION);
CREATE TABLE PUBLIC_EVENT_INFORMATION AS ( SELECT * FROM singhmk.PUBLIC_EVENT_INFORMATION);

INSERT INTO Fuser(fuid,first_name,last_name,y_birth,m_birth,d_birth,gender)
SELECT DISTINCT USER_ID, FIRST_NAME, LAST_NAME, YEAR_OF_BIRTH, MONTH_OF_BIRTH, DAY_OF_BIRTH, GENDER FROM PUBLIC_USER_INFORMATION;

create sequence lsequence start with 1 increment by 1;
CREATE TRIGGER loc_trigger
BEFORE INSERT ON Location
FOR EACH ROW
BEGIN
SELECT lsequence.NEXTVAL INTO :new.lid from dual;
END;
.
RUN;

INSERT INTO Location(city,state,country)
SELECT DISTINCT hometown_city, hometown_state, hometown_country FROM public_user_information
UNION
SELECT DISTINCT current_city, current_state, current_country FROM public_user_information
UNION
SELECT DISTINCT event_city, event_state, event_country FROM public_event_information;
drop sequence lsequence;
create sequence psequence start with 1 increment by 1;
CREATE TRIGGER pro_trigger
BEFORE INSERT ON Program
FOR EACH ROW
BEGIN
SELECT psequence.NEXTVAL INTO :new.pid from dual;
END;
.
RUN;

INSERT INTO Program(iname, year, concen, degree)
SELECT DISTINCT INSTITUTION_NAME,PROGRAM_YEAR,PROGRAM_CONCENTRATION,PROGRAM_DEGREE FROM PUBLIC_USER_INFORMATION;
drop sequence psequence;

INSERT INTO Friends(fuid_1, fuid_2)
SELECT DISTINCT USER1_ID, USER2_ID FROM PUBLIC_ARE_FRIENDS F ORDER BY F.USER1_ID ASC, F.USER2_ID ASC;

INSERT INTO Hometowns(fuid, lid)
SELECT DISTINCT U.USER_ID, L.lid
FROM PUBLIC_USER_INFORMATION U, Location L
WHERE U.hometown_city = L.city AND U.hometown_state = L.state AND U.hometown_country = L.country;
NSERT INTO Residents(fuid, lid)
SELECT DISTINCT U.USER_ID, L.lid
FROM PUBLIC_USER_INFORMATION U, Location L
WHERE U.current_city = L.city AND U.current_state = L.state
AND U.current_country = L.country;

INSERT INTO Educates(fuid, pid)
SELECT DISTINCT U.USER_ID, P.pid
FROM PUBLIC_USER_INFORMATION U, Program P
WHERE U.institution_name = P.iname AND U.program_year = P.year
AND U.program_concentration = P.concen AND U.program_degree = P.degree;

SET AUTOCOMMIT OFF
INSERT INTO Album_Covers(aid,aname,time_create,time_modify,link,visibility,pid)
SELECT DISTINCT album_id,album_name,album_created_time,album_modified_time,album_link,album_visibility,cover_photo_id
FROM public_photo_information;
INSERT INTO Photo_Belongs(pid,caption,time_create,time_modify,link,aid)
SELECT DISTINCT photo_id,photo_caption,photo_created_time,photo_modified_time,photo_link,album_id
From public_photo_information;
COMMIT
SET AUTOCOMMIT ON

INSERT INTO Tags(pid,fuid,time_create,x,y)
SELECT DISTINCT photo_id,tag_subject_id,tag_created_time,tag_x_coordinate,tag_y_coordinate
FROM public_tag_information;

INSERT INTO Owns(fuid,aid)
SELECT DISTINCT owner_id,album_id
FROM public_photo_information;

INSERT INTO Event_Creates(eid,ename,tagline,descrip,host,etype,subtype,time_start,time_end,fuid)
SELECT DISTINCT event_id,event_name,event_tagline,event_description,event_host,event_type,event_subtype,event_start_time,event_end_time,event_creator_id
FROM public_event_information;

--INSERT INTO Participates(fuid,eid,status)

INSERT INTO Locates(eid,lid,descrip)
SELECT DISTINCT E.EVENT_ID, L.lid, E.EVENT_LOCATION
FROM PUBLIC_EVENT_INFORMATION E, Location L
WHERE E.event_city = L.city AND E.event_state = L.state
AND E.event_country = L.country;
