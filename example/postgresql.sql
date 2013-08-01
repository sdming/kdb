-- Table: ttable

-- DROP TABLE ttable;

CREATE TABLE ttable
(
  pk bigserial NOT NULL,
  cbool boolean,
  cint integer,
  cfloat double precision,
  cnumeric numeric(10,4),
  cstring character varying(100),
  cdate date,
  cdatetime timestamp without time zone,
  cguid uuid,
  cbytes bit varying(1000),
  CONSTRAINT ttable_pkey PRIMARY KEY (pk)
);

-- Function: fn_query(integer)

-- DROP FUNCTION fn_query(integer);

CREATE OR REPLACE FUNCTION fn_query(cint integer)
  RETURNS SETOF ttable AS
$BODY$
BEGIN
  RETURN QUERY SELECT * FROM public.ttable t WHERE t.cint > $1;
END;
$BODY$
  LANGUAGE plpgsql ;

-- Function: fn_exec(integer)

-- DROP FUNCTION fn_exec(integer);

CREATE OR REPLACE FUNCTION fn_exec(cint integer)
  RETURNS integer AS
$BODY$
DECLARE
  affected_rows INT DEFAULT 0;
BEGIN

  DELETE FROM public.ttable t  WHERE t.cint = $1;  
  GET DIAGNOSTICS affected_rows = ROW_COUNT;  
  RETURN affected_rows;
END $BODY$
  LANGUAGE plpgsql;

-- Function: fn_inout(integer, integer)

-- DROP FUNCTION fn_inout(integer, integer);

CREATE OR REPLACE FUNCTION fn_inout(IN x integer, INOUT y integer, OUT sum integer)
  RETURNS record AS
$BODY$
BEGIN
    sum := x + y;
    y := 2 * y;
END;
$BODY$
  LANGUAGE plpgsql ;

