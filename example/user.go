package main

import (
	"fmt"
	_ "github.com/go-sql-driver/mysql"
	"github.com/sdming/kdb"
	"log"
	"math/rand"
	"os"
	"time"
)

/*
CREATE TABLE `t_users` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `name` varchar(100) DEFAULT NULL,
  `gender` int(11) DEFAULT NULL,
  `address` varchar(500) DEFAULT NULL,
  `height` int(11) DEFAULT NULL,
  `Weight` decimal(10,2) DEFAULT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

*/

type Gender int

type User struct {
	Id     int    "kdb:{pk,readonly}"
	Name   string "kdb:{readonly}"
	Gender Gender
	Addr   string "kdb:{name=address}"
	Height int32
	Weight float32
}

var logger *log.Logger = log.New(os.Stdout, "kdb", log.Ldate|log.Ltime)

func init() {
	kdb.RegisterDSN("demo", "mysql", "data:data@tcp(172.18.194.136:3306)/demo")
	kdb.Logger = logger
	kdb.LogLevel = kdb.LogDebug
	rand.Seed(int64(time.Now().Nanosecond()))
}

func test() {
	table := "t_users"

	db := kdb.NewDB("demo")
	defer db.Close()

	name := fmt.Sprintf("name_%d", rand.Int31n(100))
	user := User{
		Name:   name,
		Gender: Gender(rand.Int31n(5)),
		Addr:   fmt.Sprintf("%s address", name),
		Height: 100 + rand.Int31n(100),
		Weight: 25 + 50*rand.Float32(),
	}

	logger.Println("user", user)

	logger.Println("insert")
	result, err := db.Insert(table, kdb.Entity(user, "pk"))
	if err != nil {
		logger.Fatal(err)
	}
	logger.Print("insert result:")
	logger.Println(result.LastInsertId())

	logger.Println("select")
	rows, err := db.SelectAll(table, "name", kdb.Equals, name)
	if err != nil {
		logger.Fatal(err)
	}
	logger.Println(rows.Columns())

	logger.Println("convert")
	var users []User
	err = kdb.Read(rows, &users)
	if err != nil {
		logger.Fatal(err)
	}
	logger.Println("users", users)

	for _, u := range users {
		u.Height = 10 + u.Height
		u.Weight = 11.11 + u.Weight
		u.Addr = u.Addr + "-new"
		u.Name = u.Name + "-new"
		u.Gender = u.Gender + 1

		logger.Println("update", u.Id)
		result, err = db.Update(table, kdb.Entity(u, "readonly"), "id", kdb.Equals, u.Id)
		if err != nil {
			logger.Fatal(err)
		}
		logger.Print("update result:")
		logger.Println(result.RowsAffected())
	}

	logger.Println("delete")
	result, err = db.DeleteByCol(table, "name", name)
	if err != nil {
		logger.Fatal(err)
	}
	logger.Print("delete result:")
	logger.Println(result.RowsAffected())

	logger.Println("exists", name)
	logger.Println(db.SelectExists(table, "name", kdb.Equals, name))

}

func main() {
	test()
}
