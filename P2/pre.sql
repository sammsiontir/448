DROP TABLE PUBLIC_USER;
DROP TABLE PUBLIC_FRIENDS;
DROP TABLE PUBLIC_CITIES;
DROP TABLE PUBLIC_PROGRAMS;
DROP TABLE PUBLIC_USER_CURRENT_CITY;
DROP TABLE PUBLIC_USER_HOMETOWN_CITY;
DROP TABLE PUBLIC_EDUCATION;
DROP TABLE PUBLIC_USER_EVENTS;
DROP TABLE PUBLIC_PHOTOS;
DROP TABLE PUBLIC_ALBUMS;
DROP TABLE PUBLIC_TAGS;
DROP TABLE PUBLIC_PARTICIPANTS;
CREATE TABLE PUBLIC_USER AS ( SELECT * FROM singhmk.PUBLIC_USERS);
CREATE TABLE PUBLIC_FRIENDS AS ( SELECT * FROM singhmk.PUBLIC_FRIENDS);
CREATE TABLE PUBLIC_CITIES AS ( SELECT * FROM singhmk.PUBLIC_CITIES);
CREATE TABLE PUBLIC_PROGRAMS AS ( SELECT * FROM singhmk.PUBLIC_PROGRAMS);
CREATE TABLE PUBLIC_USER_CURRENT_CITY AS ( SELECT * FROM singhmk.PUBLIC_USER_CURRENT_CITY);
CREATE TABLE PUBLIC_USER_HOMETOWN_CITY AS ( SELECT * FROM singhmk.PUBLIC_USER_HOMETOWN_CITY);
CREATE TABLE PUBLIC_EDUCATION AS ( SELECT * FROM singhmk.PUBLIC_EDUCATION);
CREATE TABLE PUBLIC_USER_EVENTS AS ( SELECT * FROM singhmk.PUBLIC_USER_EVENTS);
CREATE TABLE PUBLIC_PHOTOS AS ( SELECT * FROM singhmk.PUBLIC_PHOTOS);
CREATE TABLE PUBLIC_ALBUMS AS ( SELECT * FROM singhmk.PUBLIC_ALBUMS);
CREATE TABLE PUBLIC_TAGS AS ( SELECT * FROM singhmk.PUBLIC_TAGS);
CREATE TABLE PUBLIC_PARTICIPANTS AS ( SELECT * FROM singhmk.PUBLIC_PARTICIPANTS);

