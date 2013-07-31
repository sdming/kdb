package kdb

import (
	"regexp"
	"strings"
	"testing"
	"time"
)

func removeSpace(text string) string {
	re := regexp.MustCompile(`\s`)
	return re.ReplaceAllString(text, "")
}

var dataTypeMap map[string]interface{} = map[string]interface{}{
	"cbool":     true,
	"cint":      42,
	"cfloat":    3.14,
	"cnumeric":  1.1,
	"cstring":   "string",
	"cdate":     "2004-07-24",
	"cdatetime": time.Now(),
	"cguid":     "550e8400-e29b-41d4-a716-446655440000",
}

func orderby(od *OrderBy) {
	od.Asc("cint", "cfloat")
	od.Desc("cnumeric", "cstring")
	od.By(Asc, Column("cdatetime"))
}

func where(w *Where) {
	w.Equals("cbool", true).
		NotEquals("cbool", false)

	w.LessThan("cstring", "LessThan").
		LessOrEquals("cstring", "LessOrEquals").
		GreaterThan("cstring", "GreaterThan").
		GreaterOrEquals("cstring", "GreaterOrEquals").
		Equals("cstring", "Equals").
		NotEquals("cstring", "NotEquals").
		In("cstring", []string{"a", "b", "c"}).
		NotIn("cstring", [3]string{"h", "i", "j"}).
		Like("cstring", "%like%").
		NotLike("cstring", "%NotLike%")

	w.LessThan("cint", 100).
		LessOrEquals("cint", 101).
		GreaterThan("cint", 200).
		GreaterOrEquals("cint", 201).
		Equals("cint", 300).
		NotEquals("cint", 301).
		In("cint", []int{0, 1, 2, 3, 4}).
		NotIn("cint", [5]int{5, 6, 7, 8, 9})

	w.LessThan("cfloat", 1.01).
		LessOrEquals("cfloat", 1.02).
		GreaterThan("cfloat", 2.01).
		GreaterOrEquals("cfloat", 2.02).
		Equals("cfloat", 3.01).
		NotEquals("cfloat", 3.02).
		In("cfloat", []float64{10.01, 11.01, 12.01, 13.01, 14.01}).
		NotIn("cfloat", [5]float64{15.01, 16.01, 17.01, 18.01, 19.01})

	w.LessThan("cnumeric", 1.1).
		LessOrEquals("cnumeric", 1.2).
		GreaterThan("cnumeric", 2.1).
		GreaterOrEquals("cnumeric", 2.2).
		Equals("cnumeric", 3.1).
		NotEquals("cnumeric", 3.2).
		In("cnumeric", []float32{10.1, 11.1, 12.1, 13.1, 14.1}).
		NotIn("cnumeric", [5]float32{15.1, 16.1, 17.1, 18.1, 19.1})

	w.LessThan("cdate", "2000-01-01").
		LessOrEquals("cdate", "2000-01-02").
		GreaterThan("cdate", "2000-02-01").
		GreaterOrEquals("cdate", "2000-02-02").
		Equals("cdate", "2000-03-01").
		NotEquals("cdate", "2000-03-02").
		In("cdate", []string{"2000-04-01", "2000-04-02"}).
		NotIn("cdate", [2]string{"2000-05-01", "2000-05-02"})

	w.LessThan("cdatetime", "2001-01-01").
		LessOrEquals("cdatetime", "2001-01-02").
		GreaterThan("cdatetime", "2001-02-01").
		GreaterOrEquals("cdatetime", "2001-02-02").
		Equals("cdatetime", "2001-03-01").
		NotEquals("cdatetime", "2001-03-02").
		In("cdatetime", []string{"2001-04-01 01:01:01", "2001-04-02 02:02:02"}).
		NotIn("cdatetime", [2]time.Time{time.Now(), time.Now().Add(time.Hour)})

	w.Equals("cguid", "550e8400-e29b-41d4-a716-446655440000")

	w.OpenParentheses().
		OpenParentheses().
		IsNull("cbytes").
		Or().
		IsNotNull("cbytes").
		CloseParentheses().
		Or().
		OpenParentheses().
		Sql(" 1!=2 ").
		Exists(Sql("select count(*) from ttable where cint > 1")).
		NotExists(Sql("select count(*) from ttable where cint > 10000")).
		In("cint", Sql("select cint from ttable")).
		NotIn("cint", Sql("select cint from ttable")).
		CloseParentheses().
		CloseParentheses()

}

func TestQuery(t *testing.T) {
	comiler, err := GetCompiler("ansi")
	if err != nil {
		t.Error("can not find ansi compiler", err)
	}

	var q *Query

	q = NewQuery("ttable", "t1").Distinct()

	q.Select.Column("cbool", "t1.cint").
		ColumnAs("cnumeric", "a_cnumeric").
		ColumnAs("t1.cstring", "a_cstring").
		Avg("cint", "avg_cint").
		Count("t1.cint", "count_cint").
		Sum("cint", "sum_cint").
		Min("t1.cint", "min_cint").
		Max("cint", "max_cint").
		Exp(Sql("cint - 1"), "exp_cint")

	//q.From.ThenFrom("ttable_c", "t2")
	q.From.CrossJoin("ttable_c", "t_c").Equals("t1.cint", Column("t_c.c_int"))
	q.From.InnerJoin("ttable_c", "t_i").On("t1.cstring", "t_i.c_string")
	q.From.LeftJoin("ttable_c", "t_l").On("t1.cstring", "t_l.c_string")
	q.From.RightJoin("ttable_c", "t_r").On2("t1.cstring", "t_r.c_string", "t1.cint", "t_r.c_int")

	where(q.Where)

	q.UseGroupBy().
		Column("cbool", "t1.cint", "cnumeric", "t1.cstring").
		By(Sql("cint - 1"))

	q.UseHaving().
		Like("t1.cstring", "%like%").
		NotIn("cint", []int{1, 2, 3, 4, 5}).
		OpenParentheses().
		LessThan("cint", 12345).
		Or().
		GreaterOrEquals("cint", 101).
		CloseParentheses().
		Equals("cnumeric", 1.1).
		IsNotNull("cbool")

	q.UseHaving().
		Avg(LessThan, "cint", 201).
		Count(GreaterThan, "cint", 301).
		Sum(NotEquals, "cint", 401).
		Min(LessOrEquals, "cint", 501).
		Max(GreaterOrEquals, "cint", 601)

	orderby(q.UseOrderBy())

	q.Limit(3, 101)

	t.Log(q)

	formatedSql, args, err := comiler.Compile("source", q)
	t.Log(formatedSql, args)

	if err != nil {
		t.Error("compile query error", err)
	}

	var want string = `
SELECT DISTINCT cbool, t1.cint, cnumeric AS 'a_cnumeric', t1.cstring AS 'a_cstring', AVG(cint) AS 'avg_cint', COUNT(t1.cint) AS 'count_cint', SUM(cint) AS 'sum_cint', MIN(t1.cint) AS 'min_cint', MAX(cint) AS 'max_cint', cint - 1 AS 'exp_cint' 
FROM ttable AS t1
CROSS JOIN ttable_c AS t_c ON t1.cint = t_c.c_int 
INNER JOIN ttable_c AS t_i ON t1.cstring = t_i.c_string 
LEFT JOIN ttable_c AS t_l ON t1.cstring = t_l.c_string 
RIGHT JOIN ttable_c AS t_r ON t1.cstring = t_r.c_string  AND  t1.cint = t_r.c_int  
WHERE
cbool =  ? 
AND
cbool <>  ? 
AND
cstring <  ? 
AND
cstring <=  ? 
AND
cstring >  ? 
AND
cstring >=  ? 
AND
cstring =  ? 
AND
cstring <>  ? 
AND
cstring IN ( ? ,  ? ,  ? )
AND
cstring NOT IN ( ? ,  ? ,  ? )
AND
cstring LIKE  ? 
AND
cstring NOT LIKE  ? 
AND
cint <  ? 
AND
cint <=  ? 
AND
cint >  ? 
AND
cint >=  ? 
AND
cint =  ? 
AND
cint <>  ? 
AND
cint IN (0, 1, 2, 3, 4)
AND
cint NOT IN ( ? ,  ? ,  ? ,  ? ,  ? )
AND
cfloat <  ? 
AND
cfloat <=  ? 
AND
cfloat >  ? 
AND
cfloat >=  ? 
AND
cfloat =  ? 
AND
cfloat <>  ? 
AND
cfloat IN (10.01, 11.01, 12.01, 13.01, 14.01)
AND
cfloat NOT IN ( ? ,  ? ,  ? ,  ? ,  ? )
AND
cnumeric <  ? 
AND
cnumeric <=  ? 
AND
cnumeric >  ? 
AND
cnumeric >=  ? 
AND
cnumeric =  ? 
AND
cnumeric <>  ? 
AND
cnumeric IN (10.1, 11.1, 12.1, 13.1, 14.1)
AND
cnumeric NOT IN ( ? ,  ? ,  ? ,  ? ,  ? )
AND
cdate <  ? 
AND
cdate <=  ? 
AND
cdate >  ? 
AND
cdate >=  ? 
AND
cdate =  ? 
AND
cdate <>  ? 
AND
cdate IN ( ? ,  ? )
AND
cdate NOT IN ( ? ,  ? )
AND
cdatetime <  ? 
AND
cdatetime <=  ? 
AND
cdatetime >  ? 
AND
cdatetime >=  ? 
AND
cdatetime =  ? 
AND
cdatetime <>  ? 
AND
cdatetime IN ( ? ,  ? )
AND
cdatetime NOT IN ( ? ,  ? )
AND
cguid =  ? 
AND
(
	(
		cbytes IS NULL
		OR
		cbytes IS NOT NULL
	)
	OR
	(
		 1!=2 
		AND
		EXISTS(select count(*) from ttable where cint > 1)
		AND
		NOT EXISTS(select count(*) from ttable where cint > 10000)
		AND
		cint IN (select cint from ttable)
		AND
		cint NOT IN (select cint from ttable)
	)
) 
GROUP BY cbool, t1.cint, cnumeric, t1.cstring, cint - 1 
HAVING
t1.cstring LIKE  ? 
AND
cint NOT IN (1, 2, 3, 4, 5)
AND
(
	cint <  ? 
	OR
	cint >=  ? 
)
AND
cnumeric =  ? 
AND
cbool IS NOT NULL
AND
AVG(cint) <  ? 
AND
COUNT(cint) >  ? 
AND
SUM(cint) <>  ? 
AND
MIN(cint) <=  ? 
AND
MAX(cint) >=  ?  
ORDER BY cint ASC, cfloat ASC, cnumeric DESC, cstring DESC, cdatetime ASC 
LIMIT 3,101;
`

	if !strings.EqualFold(removeSpace(formatedSql), removeSpace(want)) {
		t.Error("compiled query sql error")
	}
}

func TestText(t *testing.T) {
	var text *Text

	text = NewText(`
select * 
from ttable 
where 
	cbool = {cbool}
	and cint > {cint}
	and cfloat < {cfloat} 
	and cnumeric <> {cnumeric}
	and cstring like {cstring} 
	and cdate = {cdate}
	and cdatetime = {cdatetime}
	and cbytes is null 
	and cguid = {cguid} 
`)

	for k, v := range dataTypeMap {
		text.Set(k, v)
	}

	comiler, err := GetCompiler("ansi")
	if err != nil {
		t.Error("can not find ansi compiler", err)
	}
	formatedSql, args, err := comiler.Compile("source", text)
	t.Log(formatedSql, args)
	if err != nil {
		t.Error("compile text error", err)
	}

	var want string = `
select * 
from ttable 
where 
	cbool =  ? 
	and cint >  ? 
	and cfloat <  ?  
	and cnumeric <>  ? 
	and cstring like  ?  
	and cdate =  ? 
	and cdatetime =  ? 
	and cbytes is null 
	and cguid =  ?  
`

	if !strings.EqualFold(removeSpace(formatedSql), removeSpace(want)) {
		t.Error("compiled text sql error")
	}

}

func TestProcedure(t *testing.T) {
	var p *Procedure

	p = NewProcedure("sp_types")
	p.Set("cbool", true)
	p.Set("cint", 123)
	p.Set("cfloat", 3.14)
	p.Set("cnumeric", 101.101)
	p.Set("cdate", "2004-07-24")
	p.Set("cdatetime", "2013-01-01 01:02:03")

	comiler, err := GetCompiler("ansi")
	if err != nil {
		t.Error("can not find ansi compiler", err)
	}

	formatedSql, args, err := comiler.Compile("source", p)
	t.Log(formatedSql, args)
	if err != nil {
		t.Error("compile procedure error", err)
	}

	var want string = `
call sp_types(?,?,?,?,?,?);
`

	if !strings.EqualFold(removeSpace(formatedSql), removeSpace(want)) {
		t.Error("compiled procedure sql error")
	}

}

func TestUpdate(t *testing.T) {
	var u *Update

	u = NewUpdate("ttable")
	for k, v := range dataTypeMap {
		u.Set(k, v)
	}
	u.Where.Equals("cint", 101)
	u.OrderBy.Asc("cint")
	u.Limit(101)

	comiler, err := GetCompiler("ansi")
	if err != nil {
		t.Error("can not find ansi compiler", err)
	}

	formatedSql, args, err := comiler.Compile("source", u)
	t.Log(formatedSql, args)
	if err != nil {
		t.Error("compile update error", err)
	}

	var want string = `
UPDATE ttable SET 
cbool=? , cint=? , cfloat=? , cnumeric=? , cstring=? , cdate=? , cdatetime=? , cguid=? 
WHERE
cint = ?  
ORDER BY cint ASC 
LIMIT 101;
`
	if !strings.EqualFold(removeSpace(formatedSql), removeSpace(want)) {
		t.Error("compiled update sql error")
	}

}

func TestDelete(t *testing.T) {
	var d *Delete

	d = NewDelete("ttable")
	d.Where.Equals("cint", 101)
	d.OrderBy.Asc("cint")
	d.Limit(101)

	comiler, err := GetCompiler("ansi")
	if err != nil {
		t.Error("can not find ansi compiler", err)
	}

	formatedSql, args, err := comiler.Compile("source", d)
	t.Log(formatedSql, args)
	if err != nil {
		t.Error("compile delete error", err)
	}

	var want string = `
DELETE FROM ttable
WHERE
cint =  ?  
ORDER BY cint ASC 
LIMIT 101;
`
	if !strings.EqualFold(removeSpace(formatedSql), removeSpace(want)) {
		t.Error("compiled delete sql error")
	}
}

func TestInsert(t *testing.T) {
	var insert *Insert

	insert = NewInsert("ttable")
	for k, v := range dataTypeMap {
		insert.Set(k, v)
	}

	comiler, err := GetCompiler("ansi")
	if err != nil {
		t.Error("can not find ansi compiler", err)
	}

	formatedSql, args, err := comiler.Compile("source", insert)
	t.Log(formatedSql, args)
	if err != nil {
		t.Error("compile insert error", err)
	}

	var want string = `
INSERT INTO ttable(cbool, cint, cfloat, cnumeric, cstring, cdate, cdatetime, cguid)
VALUES( ? ,  ? ,  ? ,  ? ,  ? ,  ? ,  ? ,  ? );
`
	if !strings.EqualFold(removeSpace(formatedSql), removeSpace(want)) {
		t.Error("compiled insert sql error")
	}
}
