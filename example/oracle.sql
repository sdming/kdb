CREATE TABLE TTABLE 
(
  PK NUMBER NOT NULL 
, CBOOL NUMBER(*, 0) 
, CINT NUMBER(*, 0) 
, CFLOAT FLOAT(126) 
, CNUMERIC NUMBER(10, 4) 
, CSTRING NVARCHAR2(20) 
, CDATE DATE 
, CONSTRAINT TTABLE_PK PRIMARY KEY 
  (
    PK 
  )
) 
;

CREATE SEQUENCE TTABLE_PK_Sequence INCREMENT BY 1 START WITH 1 NOMAXVALUE NOCYCLE CACHE 10;

CREATE TRIGGER TTABLE_PK_Identity BEFORE INSERT ON  TTABLE FOR EACH ROW WHEN (new.PK is null)
begin
select TTABLE_PK_Sequence.nextval into:new.PK from dual;
end;


create or replace 
procedure sp_exec(v_cint in number) as  
begin  
    update TTABLE set  CFLOAT = 1.1 * v_cint where cint > v_cint;  
end;  

create or replace 
procedure sp_inout(x in number, y in out number, s out number) as  
begin  
    s := x + y;
    y := 2 * y;  
end;  

create or replace 
procedure sp_query(v_cint in number, refcur out sys_refcursor) as  
begin  

  open refcur for  
    select * from TTABLE where cint > v_cint;  
end;  

SET SERVEROUTPUT ON;

DECLARE
  x number := 1;
  y number := 2;
  s number;
begin
  DBMS_OUTPUT.PUT_LINE('x='||x||'y='||y||'s='||s);
  sp_inout(x=>x, y=>y, s=>s);
  DBMS_OUTPUT.PUT_LINE('x='||x||'y='||y||'s='||s);
end;

exec sp_exec(2);

var cur refcursor;
exec sp_query(1, :cur);
print :cur;