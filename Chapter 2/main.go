package main

import (
	"encoding/json"
	"fmt"
	"log"
	"math"
	"time"

	"github.com/catorpilor/betteruid"
	"github.com/catorpilor/redis_test/structs"
	"github.com/garyburd/redigo/redis"
)

var (
	c    redis.Conn
	quit chan bool
)

const (
	LIMIT = 1000
	TTL   = 24 * 60 * 60 //cache one day
)

func main() {
	var err error
	c, err = redis.Dial("tcp", ":6379")
	if err != nil {
		panic(err)
	}
	defer c.Close()
	quit = make(chan bool)
	//cleanUp()
	//time.Sleep(5 * 1e9)
	// quit <- true
	// next, err := redis.Values(c.Do("ZRANGE", "schedule:", 0, 0, "withscores"))
	// if err != nil {
	// 	log.Fatal(err)
	// }
	// //now := time.Now()
	// //
	// var rowId, timestamp int64
	// _, err = redis.Scan(next, &timestamp, &rowId)
	// fmt.Println(timestamp, rowId)
	uid := betteruid.New()
	//updateToken("fad717289d7d8sa78df", "user:1", "item:1dfd32343")
	updateTokenPipeline(uid, "users:1", "item:1fdaafdafdsad")
	// test, err := redis.Values(c.Do("ZRANGE", "mytest", 0, -1))
	// if err != nil {
	// 	log.Fatal(err)
	// }
	// fmt.Println("zzz")
	// tokens := make([]interface{}, len(test)+1)
	// tokens[0] = "mytest"
	// fmt.Println(tokens)
	// for i, _ := range test {
	// 	var key string
	// 	test, err = redis.Scan(test, &key)
	// 	if err != nil {
	// 		log.Fatal(err)
	// 	}
	// 	str, _ := redis.String(c.Do("GET", "login:"+key))
	// 	fmt.Println(i, str)
	// 	tokens[i+1] = key
	// }
	// fmt.Println(tokens)
	// ival, _ := redis.Int(c.Do("ZREM", tokens...))
	// fmt.Println(ival)
	// for len(test) > 0 {
	// 	var key, value string
	// 	test, err = redis.Scan(test, &key, &value)
	// 	if err != nil {
	// 		log.Fatal(err)
	// 	}
	// 	fmt.Println(key, value)
	// }
}

func checkToken(token string) string {
	str, err := redis.String(c.Do("HGET", "login:", token))
	if err != nil {
		fmt.Errorf("HGET login: token%v got err %v", token, err.Error())
		return ""
	}
	return str
}

func updateToken(token, user, item string) {
	now := time.Now()
	//usint tranaction to reduce the amount time to wait for a response
	c.Send("MULTI")
	c.Send("HSET", "login:", token, user)
	//c.Do("HSET", "login:", token, user) //keep a map from token to logged user
	//if we do not want to perform analytics on "recent:"
	//we can use EXPIRE instead
	//
	timestamp := fmt.Sprintf("%d", now.Unix())
	c.Send("SET", fmt.Sprintf("recent:%s", token), timestamp)
	c.Send("EXPIRE", fmt.Sprintf("recent:%s", token), TTL)
	//c.Do("ZADD", "recent:", timestamp, token)
	viewd := fmt.Sprintf("viewd:%s", token)
	if item != "" {
		//c.Send("ZADD", viewd, timestamp, item)
		//c.Do("ZADD", viewd, timestamp, item) //record user viewd item
		//change zset to list save memeory
		//c.Do("LPUSH", viewd)
		c.Send("LPUSH", viewd, item)
		c.Send("LTRIM", viewd, 0, 24)
		//c.Send("ZREMRANGEBYRANK", viewd, 0, -26)
		//c.Do("ZREMRANGEBYRANK", viewd, 0, -26) //remove old items keep recent 25
	}
	_, err := redis.Values(c.Do("EXEC"))
	if err != nil {
		log.Fatal(err)
	}
}

func updateTokenPipeline(token, user, item string) {
	fmt.Println(token)
	now := time.Now()
	//usint tranaction to reduce the amount time to wait for a response
	c.Send("HSET", "login:", token, user)
	//c.Do("HSET", "login:", token, user) //keep a map from token to logged user
	//if we do not want to perform analytics on "recent:"
	//we can use EXPIRE instead
	//
	timestamp := fmt.Sprintf("%d", now.Unix())
	c.Send("SET", fmt.Sprintf("recent:%s", token), timestamp)
	c.Send("EXPIRE", fmt.Sprintf("recent:%s", token), TTL)
	//c.Do("ZADD", "recent:", timestamp, token)
	viewd := fmt.Sprintf("viewd:%s", token)
	if item != "" {
		//c.Send("ZADD", viewd, timestamp, item)
		//c.Do("ZADD", viewd, timestamp, item) //record user viewd item
		//change zset to list save memeory
		//c.Do("LPUSH", viewd)
		c.Send("LPUSH", viewd, item)
		c.Send("LTRIM", viewd, 0, 24)
		//c.Send("ZREMRANGEBYRANK", viewd, 0, -26)
		//c.Do("ZREMRANGEBYRANK", viewd, 0, -26) //remove old items keep recent 25
	}
	err := c.Flush()
	if err != nil {
		log.Fatal(err)
	}
}

func cleanUp() {
	//sig is a semaphore
	//trigger
	// sig := make(chan bool, 1)
	//sig <- false
	go func() {
		for {
			select {
			case <-quit:
				//got signal to exit
				fmt.Println("Got signal")
				return
			default:
				fmt.Println("default running...")
				size, err := redis.Int(c.Do("ZCARD", "recent:"))
				fmt.Printf("size:%d ,err:%v", size, err)
				if err != nil {
					log.Fatal(err)
				}
				if size <= LIMIT {
					time.Sleep(2e9)
				} else {
					endIndex := min(size-LIMIT, 100)
					tokens, err := redis.Values(c.Do("ZRANGE", "recent:", 0, endIndex))
					if err != nil {
						log.Fatal(err)
					}
					sessKeys := make([]interface{}, 2*len(tokens))
					delTokens := make([]interface{}, len(tokens)+1)
					for idx, _ := range tokens {
						var token string
						tokens, err = redis.Scan(tokens, &token)
						if err != nil {
							log.Fatal(err)
						}
						sessKeys[idx*2] = "viewed:" + token
						sessKeys[idx*2+1] = "cart:" + token
						delTokens[idx+1] = token
					}
					//delete sessions
					c.Do("DEL", sessKeys...)
					delTokens[0] = "login:"
					c.Do("HDEL", delTokens...)
					//change to "recent:"
					delTokens[0] = "recent:"
					c.Do("ZREM", delTokens...)
				}
			}
		}
	}()
}

func addToChart(sess, item string, count int) {
	if count <= 0 {
		c.Do("HREM", "cart:"+sess, item)
	} else {
		c.Do("HSET", "cart:"+sess, item, count)
	}
}

func cacheRequest() {

}

func schedule_row_cache(row_id, delay int64) {
	c.Do("ZADD", "delay:", delay, row_id)
	now := time.Now()
	c.Do("ZADD", "schedule:", now.Unix(), row_id)
}

func cacheRows() {
	go func() {
		for {
			select {
			case <-quit:
				return
			default:
				next, err := redis.Values(c.Do("ZRANGE", "schedule:", 0, 0, "withscores"))
				if err != nil {
					log.Fatal(err)
				}
				//now := time.Now()
				//
				var rowId, timestamp int64
				_, err = redis.Scan(next, &rowId, &timestamp)
				if err != nil {
					log.Fatal(err)
				}
				now := time.Now()
				if len(next) == 0 || timestamp > now.Unix() {
					time.Sleep(5e7)
				} else {
					delay, _ := redis.Int64(c.Do("ZSCORE", "delay:", rowId))
					if delay <= 0 {
						c.Do("ZREM", "delay:", rowId)
						c.Do("ZREM", "schedule:", rowId)
						c.Do("DEL", fmt.Sprintf("inv:%d", rowId))
					} else {
						//fetch object from db
						//simulate one inv
						inv := &structs.Inv{
							"629",
							"GTab 7inch",
							"A good one to process",
						}
						val, _ := json.Marshal(inv)
						c.Do("ZADD", "schedule:", now.Unix()+delay, rowId)
						c.Do("SET", fmt.Sprintf("inv:%d", rowId),
							val)
					}
				}
			}
		}
	}()
}

func rescaleViewd() {
	go func() {
		for {
			select {
			case <-quit:
				return
			default:
				c.Send("ZREMRANGEBYRANK", "viewed:", 20000, -1) //remove items not in top 20000
				c.Send("ZINTERSTORE", "viewed:", 1, "viewed:", "weight", 0.5)
				c.Flush()
				time.Sleep(30 * time.Minute)
			}
		}
	}()
}

func min(x, y int) int {
	return int(math.Min(float64(x), float64(y)))
}
