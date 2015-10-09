package main

import (
	"fmt"
	"strings"
	"time"

	"github.com/catorpilor/redis_test/structs"
	"github.com/garyburd/redigo/redis"
)

var (
	c redis.Conn
)

func main() {
	var err error
	c, err = redis.Dial("tcp", ":6379")
	if err != nil {
		panic(err)
	}
	defer c.Close()
	fmt.Println("hello")
	// createUser("jiao", "dfdfadafdad")
	// addInventoryByUser(2, "itemD")
	// addInventoryByUser(2, "itemE")
	// addInventoryByUser(2, "itemF")
	// addInventoryByUser(2, "itemG")

	//itemsInMarket(1, 38, "itemA")

	purchaseItem(2, "itemA.1")
}

func itemsInMarket(userID, price int64, item string) error {
	iv := fmt.Sprintf("inventory:%d", userID)
	it := fmt.Sprintf("%s.%d", item, userID)
	ticker := time.NewTicker(time.Millisecond * 500)
	go func() {
		for t := range ticker.C {
			fmt.Println(t)
			c.Send("WATCH", iv)
			c.Send("SISMEMBER", iv, item)
			c.Flush()
			c.Receive()                     //watch reply
			in, _ := redis.Int(c.Receive()) //SISMEMBER
			fmt.Println(in)
			if in != 1 {
				c.Do("UNWATCH")
				return
			}
			c.Send("MULTI")
			c.Send("ZADD", "market:", fmt.Sprintf("%d", price), it)
			c.Send("SREM", iv, item)
			_, err := c.Do("EXEC")
			if err == nil {
				return
			} else {
				fmt.Println(err)
			}
		}
	}()
	time.Sleep(time.Millisecond * 1600)
	ticker.Stop()
	return nil
}

func parseItem(it string) (string, string) {
	strs := strings.Split(it, ".")
	return string(strs[0]), string(strs[1])
}

func purchaseItem(buyerID int64, mi string) {
	iv, sellerID := parseItem(mi)
	ticker := time.NewTicker(time.Millisecond * 500)
	buyer := fmt.Sprintf("user:%d", buyerID)
	seller := fmt.Sprintf("user:%s", sellerID)
	inventory := fmt.Sprintf("inventory:%d", buyerID)
	go func() {
		for t := range ticker.C {
			fmt.Println(t)
			c.Send("WATCH", "market:", buyer)
			c.Send("ZSCORE", "market:", mi)
			c.Send("HGET", buyer, "funds")
			c.Flush()
			c.Receive()
			price, _ := redis.Int(c.Receive()) //ZSCORE
			fund, _ := redis.Int(c.Receive())  //HGET
			if price > fund {
				c.Do("UNWATCH")
				return
			}
			c.Send("MULTI")
			c.Send("HINCRBY", seller, "funds", price)
			c.Send("HINCRBY", buyer, "funds", -price)
			c.Send("ZREM", "market:", mi)
			c.Send("SADD", inventory, iv)
			_, err := c.Do("EXEC")
			if err == nil {
				return
			} else {
				fmt.Println(err)
			}
		}
	}()
	time.Sleep(time.Millisecond * 2000)
	ticker.Stop()
}

func createUser(name, pass string) error {
	user := &structs.User{
		name,
		pass,
		"0",
	}
	userId, _ := redis.Int(c.Do("INCR", "user:"))
	ru := fmt.Sprintf("user:%d", userId)
	if _, err := c.Do("HMSET",
		redis.Args{}.Add(ru).AddFlat(user)...); err != nil {
		fmt.Errorf("HMSET %v got err %v", ru, err)
		return err
	}
	return nil
}

func addInventoryByUser(userID int64, item string) {
	iv := fmt.Sprintf("inventory:%d", userID)
	c.Do("SADD", iv, item)
}
