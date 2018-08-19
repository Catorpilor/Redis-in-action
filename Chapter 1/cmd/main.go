package main

import (
	"fmt"
	"log"
	"strings"
	"time"

	"strconv"

	pool "github.com/catorpilor/Redis-in-action/redis"
	"github.com/catorpilor/Redis-in-action/structs"
	"github.com/gomodule/redigo/redis"
)

type VTYPE int

const (
	OneWeekInSeconds = 7 * 86400
	VoteScore        = 432
	ArticlesPerPage  = 5
	UpVote           = 1
	DownVote         = -1
)

func main() {
	//create a new redis connection
	p := pool.NewPool(":6379", 2)
	c := p.Get()
	defer c.Close()
	//create 5 posts
	//for i := 1; i < 6; i++ {
	//	_, err := postArticle(c, fmt.Sprintf("user:%d", i),
	//		fmt.Sprintf("Hello World_%d", i),
	//		fmt.Sprintf("t%d.tt", i),
	//	)
	//	if err != nil {
	//		fmt.Printf("postArticle Hello World_%d by user:%d got err:%s\n", i, i, err.Error())
	//	}
	//	time.Sleep(2 * time.Second)
	//}
	// fmt.Println(id)
	//voteArticle(c, "user:6", "article:3", DownVote)
	//voteArticle(c, "user:3", "article:5", DownVote)
	//voteArticle(c, "user:3", "article:5", DownVote)
	//addGroups(c, 0, []string{"ruby", "cplusplus", "golang"})
	//addGroups(c, 1, []string{"ruby", "cplusplus", "golang"})
	//addGroups(c, 2, []string{"golang"})
	//addGroups(c, 3, []string{"ruby", "cplusplus", "golang"})
	//
	res, err := getGroupArticles(c, "ruby", "score:", 1)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("getGroupArticles returns %v \n", res)
	//c.Send("MULTI")
	//c.Send("HMSET", "album:1", "title", "Red", "rating", 5)
	//c.Send("HMSET", "album:2", "title", "Earthbound", "rating", 1)
	//c.Send("HMSET", "album:3", "title", "Beat", "rating", 4)
	//c.Send("LPUSH", "albums", "1")
	//c.Send("LPUSH", "albums", "2")
	//c.Send("LPUSH", "albums", "3")
	//c.Send("SORT", "albums",
	//	"BY", "album:*->rating",
	//	"GET", "album:*->title",
	//	"GET", "album:*->rating")
	//values, err := redis.Values(c.Do("EXEC"))
	//if err != nil {
	//	fmt.Println(err)
	//	return
	//}
	//values, err := redis.Values(c.Do("SORT", "albums",
	//	"BY", "album:*->rating",
	//	"GET", "album:*->title",
	//	"GET", "album:*->rating"))
	//fmt.Printf("without multi/exec returns %v\n", values)
	//if err != nil {
	//	fmt.Println(err)
	//	return
	//}

	//var albums []struct {
	//	Title  string
	//	Rating int
	//}
	//if err := redis.ScanSlice(values, &albums); err != nil {
	//	fmt.Println(err)
	//	return
	//}
	//fmt.Printf("%v\n", albums)

}

func postArticle(c redis.Conn, user, title, link string) (int, error) {
	articleID, _ := redis.Int(c.Do("INCR", "article:"))
	voted := fmt.Sprintf("voted_up:%d", articleID)
	article := fmt.Sprintf("article:%d", articleID)
	c.Do("SADD", voted, user)
	c.Do("EXPIRE", voted, OneWeekInSeconds)
	now := time.Now()
	p := &structs.Article{Title: title, Link: link, Poster: user, PostAt: strconv.FormatInt(now.Unix(), 10), Votes: "1"}
	if _, err := c.Do("HMSET",
		redis.Args{}.Add(article).AddFlat(p)...); err != nil {
		return -1, fmt.Errorf("HMSET %v got err %v", article, err)
	}
	c.Do("ZADD", "score:", VoteScore, article)
	c.Do("ZADD", "time:", now.Unix(), article)
	return articleID, nil
}

func voteArticle(c redis.Conn, user, article string, vt VTYPE) {
	now := time.Now()
	cutoff := now.Unix() - OneWeekInSeconds
	t, _ := redis.Int64(c.Do("ZSCORE", "time:", article))
	if t < cutoff {
		return
	}
	id := getID(article)
	switch vt {
	case UpVote:
		//
		bm, _ := redis.Int(c.Do("SMOVE", "voted_down", "voted_up", user))
		if bm != 1 {
			//first vote
			b, _ := redis.Bool(c.Do("SADD", fmt.Sprintf("voted_up:%s", id), user))
			if b {
				c.Do("ZINCRBY", "score:", VoteScore, article)
				c.Do("HINCRBY", article, "votes", 1)
			} else {
				//already upvoted
				//cancel vote
				c.Do("ZINCRBY", "score:", -VoteScore, article)
				c.Do("HINCRBY", article, "votes", -1)
				c.Do("SREM", fmt.Sprintf("voted_up:%s", id), user)
			}
		} else {
			//switch from downvote to upvote
			c.Do("ZINCRBY", "score:", VoteScore, article)
			c.Do("HINCRBY", article, "votes", 1)
		}
	case DownVote:
		bm, _ := redis.Int(c.Do("SMOVE", "voted_up", "voted_down", user))
		if bm != 1 {
			//first vote
			b, _ := redis.Bool(c.Do("SADD", fmt.Sprintf("voted_down:%s", id), user))
			if b {
				c.Do("ZINCRBY", "score:", -VoteScore, article)
				c.Do("HINCRBY", article, "votes", -1)
			} else {
				//already downvoted
				//cancel vote
				c.Do("ZINCRBY", "score:", VoteScore, article)
				c.Do("HINCRBY", article, "votes", 1)
				//remove
				c.Do("SREM", fmt.Sprintf("voted_down:%s", id), user)
			}
		} else {
			//switch from upvote to downvote
			c.Do("ZINCRBY", "score:", -VoteScore, article)
			c.Do("HINCRBY", article, "votes", -1)
		}
	}

}

func getArticlesByPage(c redis.Conn, page int, order string) ([]structs.Article, error) {

	start := (page - 1) * ArticlesPerPage
	end := start + ArticlesPerPage - 1
	ids, err := redis.Values(c.Do("ZREVRANGE", order, start, end))
	if err != nil {
		return nil, err
	}
	//TODO
	//using pipeline to improve performance
	//c.Send("MULTI")
	//c.Send("COMMAND1")
	//c.Send("EXEC")
	c.Send("MULTI")
	for _, id := range ids {
		c.Send("HGETALL", id)
	}
	//articles := make([]*structs.Article, len(ids), len(ids))
	vals, err := redis.Values(c.Do("EXEC"))
	if err != nil {
		return nil, fmt.Errorf("MUTLI/EXEC got error %s", err.Error())
	}
	articles := make([]structs.Article, 0, len(vals))
	fmt.Printf("multi/exec returns %v, len(vals): %d\n", vals, len(vals))
	for _, v := range vals {
		vv, err := redis.Values(v, nil)
		if err != nil {
			fmt.Printf("transfer to Values got err: %s\n", err.Error())
			continue
		}
		var p structs.Article
		if err = redis.ScanStruct(vv, &p); err != nil {
			fmt.Printf("scanStruct %v got err: %s\n", vv, err.Error())
			continue
		}
		articles = append(articles, p)
	}
	return articles, nil
}

func addGroups(c redis.Conn, id int64, groups []string) {
	article := fmt.Sprintf("article:%d", id)
	for _, group := range groups {
		c.Do("SADD", fmt.Sprintf("group:%s", group), article)
	}
}

func removeGroups(c redis.Conn, id int64, groups []string) {
	article := fmt.Sprintf("article:%d", id)
	for _, group := range groups {
		c.Do("SREM", fmt.Sprintf("group:%s", group), article)
	}
}

func getGroupArticles(c redis.Conn, group, order string, page int) ([]structs.Article, error) {
	key := order + group
	fmt.Printf("get key: %s\n", key)
	b, _ := redis.Bool(c.Do("EXISTS", key))
	if !b {
		//not exists
		c.Do("ZINTERSTORE", key, 2, "group:"+group, order, "AGGREGATE", "MAX")
		c.Do("EXPIRE", key, 60)
	}
	return getArticlesByPage(c, page, key)
}

func getID(name string) string {
	idx := strings.Index(name, ":")
	if idx == 0 || idx == -1 {
		return ""
	}
	return name[idx+1:]
}
