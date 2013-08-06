CREATE TABLE TTABLE 
(
  PK NUMBER NOT NULL 
, CBOOL NUMBER(*, 0) 
, CINT NUMBER(*, 0) 
, CFLOAT FLOAT(126) 
, CNUMERIC NUMBER(10, 4) 
, CSTRING NVARCHAR2(100) 
, CDATE DATE 
, CONSTRAINT TTABLE_PK PRIMARY KEY 
  (
    PK 
  )
) 
;