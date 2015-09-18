package main

import (
	"fmt"
	"strings"
	"time"

	"github.com/catorpilor/redis_test/structs"
	"github.com/garyburd/redigo/redis"
)

type VTYPE int

const (
	ONE_WEEK_IN_SECONDS = 7 * 86400
	VOTE_SCORE          = 432
	ARTICLES_PER_PAGE   = 5
	UPVOTE              = 1
	DOWNVOTE            = -1
)

var (
	c redis.Conn
)

func main() {
	//create a new redis connection
	var err error
	c, err = redis.Dial("tcp", ":6379")
	if err != nil {
		panic(err)
	}
	defer c.Close()
	//create 5 posts
	// for i := 1; i < 6; i++ {
	// 	id := post_article(fmt.Sprintf("user:%d", i),
	// 		fmt.Sprintf("Hello World_%d", i),
	// 		fmt.Sprintf("t%d.tt", i),
	// 	)
	// 	time.Sleep(2000)
	// 	fmt.Println("Posted Article ", id)
	// }
	// fmt.Println(id)
	article_vote("user:6", "article:3", DOWNVOTE)
	// article_vote("user:3", "article:5", DOWNVOTE)
	// article_vote("user:3", "article:5", DOWNVOTE)

	// res := getGroupArticles("ruby", "score:", 1)
	// for _, v := range res {
	// 	fmt.Println(*v)
	// }

}

func post_article(user, title, link string) int {
	articleID, _ := redis.Int(c.Do("INCR", "article:"))
	voted := fmt.Sprintf("voted_up:%d", articleID)
	article := fmt.Sprintf("article:%d", articleID)
	c.Do("SADD", voted, user)
	c.Do("EXPIRE", voted, ONE_WEEK_IN_SECONDS)
	now := time.Now()
	p := &structs.Article{
		title,
		link,
		user,
		fmt.Sprintf("%d", now.Unix()),
		"1",
	}
	if _, err := c.Do("HMSET",
		redis.Args{}.Add(article).AddFlat(p)...); err != nil {
		fmt.Errorf("HMSET %v got err %v", article, err)
		return -1
	}
	c.Do("ZADD", "score:", VOTE_SCORE, article)
	c.Do("ZADD", "time:", now.Unix(), article)
	return articleID
}

func article_vote(user, article string, vt VTYPE) {
	now := time.Now()
	cutoff := now.Unix() - ONE_WEEK_IN_SECONDS
	t, _ := redis.Int64(c.Do("ZSCORE", "time:", article))
	if t < cutoff {
		return
	}
	id := getID(article)
	switch vt {
	case UPVOTE:
		//
		bm, _ := redis.Int(c.Do("SMOVE", "voted_down", "voted_up", user))
		if bm != 1 {
			//first vote
			b, _ := redis.Bool(c.Do("SADD", fmt.Sprintf("voted_up:%s", id), user))
			if b {
				c.Do("ZINCRBY", "score:", VOTE_SCORE, article)
				c.Do("HINCRBY", article, "votes", 1)
			} else {
				//already upvoted
				//cancel vote
				c.Do("ZINCRBY", "score:", -VOTE_SCORE, article)
				c.Do("HINCRBY", article, "votes", -1)
				c.Do("SREM", fmt.Sprintf("voted_up:%s", id), user)
			}
		} else {
			//switch from downvote to upvote
			c.Do("ZINCRBY", "score:", VOTE_SCORE, article)
			c.Do("HINCRBY", article, "votes", 1)
		}
	case DOWNVOTE:
		bm, _ := redis.Int(c.Do("SMOVE", "voted_up", "voted_down", user))
		if bm != 1 {
			//first vote
			b, _ := redis.Bool(c.Do("SADD", fmt.Sprintf("voted_down:%s", id), user))
			if b {
				c.Do("ZINCRBY", "score:", -VOTE_SCORE, article)
				c.Do("HINCRBY", article, "votes", -1)
			} else {
				//already downvoted
				//cancel vote
				c.Do("ZINCRBY", "score:", VOTE_SCORE, article)
				c.Do("HINCRBY", article, "votes", 1)
				//remove
				c.Do("SREM", fmt.Sprintf("voted_down:%s", id), user)
			}
		} else {
			//switch from upvote to downvote
			c.Do("ZINCRBY", "score:", -VOTE_SCORE, article)
			c.Do("HINCRBY", article, "votes", -1)
		}
	}

}

func getArticlesByPage(page int, order string) (articles []*structs.Article) {
	start := (page - 1) * ARTICLES_PER_PAGE
	end := start + ARTICLES_PER_PAGE - 1
	ids, err := redis.Values(c.Do("ZREVRANGE", order, start, end))
	if err != nil {
		fmt.Errorf("ZREVRAGE %v %v %v got error %v", order,
			start,
			end,
			err.Error())
		return
	}
	for _, id := range ids {
		v, err := redis.Values(c.Do("HGETALL", id))
		if err != nil {
			fmt.Errorf("HGETALL %v get error %v", id, err.Error())
			continue
		}
		p := &structs.Article{}
		if err := redis.ScanStruct(v, p); err != nil {
			fmt.Errorf("ScanStruct got error %v", err.Error())
			continue
		}
		articles = append(articles, p)
	}
	return
}

func addGroups(id int64, groups []string) {
	article := fmt.Sprintf("article:%d", id)
	for _, group := range groups {
		c.Do("SADD", fmt.Sprintf("group:%s", group), article)
	}
}

func removeGroups(id int64, groups []string) {
	article := fmt.Sprintf("article:%d", id)
	for _, group := range groups {
		c.Do("SREM", fmt.Sprintf("group:%s", group), article)
	}
}

func getGroupArticles(group, order string, page int) []*structs.Article {
	key := order + group
	b, _ := redis.Bool(c.Do("EXISTS", key))
	if !b {
		//not exists
		c.Do("ZINTERSTORE", key, 2, "group:"+group, order, "AGGREGATE", "MAX")
		c.Do("EXPIRE", key, 60)
	}
	return getArticlesByPage(page, key)
}

func getID(name string) string {
	idx := strings.Index(name, ":")
	if idx == 0 || idx == -1 {
		return ""
	}
	return name[idx+1:]
}
