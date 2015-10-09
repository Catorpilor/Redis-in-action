package structs

type Article struct {
	Title  string `redis:"title"`
	Link   string `redis:"link"`
	Poster string `redis:"poster"`
	PostAt string `redis:"time"`
	Votes  string `redis:"votes"`
}

type User struct {
	Name     string `redis:"name"`
	Password string `redis:"password"`
	Funds    string `redis:"funds"`
}

type Inv struct {
	Quantity    string `redis:'qty'`
	Name        string `redis:'name'`
	Description string `redis:'description'`
}
