DBMS Project
================
C program that creates a simple DBMS

Functions:

* list table
* list schema for tablename
* create table tablename({colname coltype [not null]})
* insert into table values({values}) - full tuple insert only
* drop table tablename
* select * from tablename
* update tablename set colname = datavalue [where colname {<>=} datavalue]
* delete from tablename [where colname {<>=} datavalue]
* select colname from tablename
* select colname from table where colname {<>=} datavalue [and/or] colname {<>=} datavalue
* select colname from table where colname is [not] null [and/or] colname is [not] null (or combined with above)
