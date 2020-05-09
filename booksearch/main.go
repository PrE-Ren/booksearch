package main

import (
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"strconv"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/olivere/elastic"
	"github.com/teris-io/shortid"
	"golang.org/x/net/html"
)

const (
	elasticIndexName = "books"
	elasticTypeName  = "book"
	baseUrlTitle     = "http://www.gutenberg.org/ebooks/"
	baseUrlContent   = "http://www.gutenberg.org/0/"
)

type Book struct {
	ID        string    `json:"id"`
	Title     string    `json:"title"`
	CreatedAt time.Time `json:"created_at"`
	Content   string    `json:"content"`
}

type CreateBookRequest struct {
	Title   string `json:"title"`
	Content string `json:"content"`
}

type SearchResponse struct {
	Time  string `json:"time"`
	Hits  string `json:"hits"`
	Books []Book `json:"books"`
}

var (
	elasticClient *elastic.Client
	startIndex    int
)

func main() {
	var err error
	ticker := time.NewTicker(1 * time.Minute)
	done := make(chan bool)
	go func() {
		for {
			select {
			case <-done:
				return
			case <-ticker.C:
				crawlBooks()
			}
		}
	}()

	for {
		elasticClient, err = elastic.NewClient(
			elastic.SetURL("http://elasticsearch:9200"),
			elastic.SetSniff(false),
		)
		if err != nil {
			log.Println(err)
			time.Sleep(3 * time.Second)
		} else {
			startIndex = 1
			break
		}
	}

	r := gin.Default()
	r.POST("/books", createBookEndpoint)
	r.GET("/books", getBookEndpoint)
	r.GET("/search", searchEndpoint)
	if err = r.Run(":8080"); err != nil {
		log.Fatal(err)
	}

	time.Sleep(20 * time.Minute)
	ticker.Stop()
	done <- true
}

func crawlBooks() {
	var index int
	for index = startIndex; index < startIndex+10; index++ {
		url := baseUrlTitle + strconv.Itoa(index)
		res, err := http.Get(url)
		log.Println("at index:", index)
		if err != nil {
			log.Println("can't get url data")
			return
		}
		defer res.Body.Close()
		title := extractTitle(res.Body)
		log.Println(title)
	}
	startIndex = index
}

func extractTitle(res io.Reader) string {
	tokenizer := html.NewTokenizer(res)
	for {
		token := tokenizer.Next()
		switch token {
		case html.StartTagToken, html.SelfClosingTagToken:
			tag := tokenizer.Token()
			if tag.Data == "meta" {
				ogTitle, ok := extractMetaProperty(tag, "og:title")
				if ok {
					return ogTitle
				}
			}
		}
	}
}

func extractMetaProperty(tag html.Token, prop string) (content string, ok bool) {
	for _, attr := range tag.Attr {
		if attr.Key == "property" && attr.Val == prop {
			ok = true
		}

		if attr.Key == "content" {
			content = attr.Val
		}
	}
	return
}

func getBookEndpoint(c *gin.Context) {
	id := c.Query("id")
	if id == "" {
		errorResponse(c, http.StatusBadRequest, "Id not specified")
		return
	}
	res, err := elasticClient.Get().Index("books").Type("book").Id(id).Do(c)
	if err != nil {
		log.Println(err)
		errorResponse(c, http.StatusInternalServerError, err.Error())
		return
	}
	c.JSON(http.StatusOK, res.Source)
}

func createBookEndpoint(c *gin.Context) {
	var req CreateBookRequest
	if err := c.BindJSON(&req); err != nil {
		errorResponse(c, http.StatusBadRequest, "Malformed request body")
		return
	}

	book := Book{
		ID:        shortid.MustGenerate(),
		Title:     req.Title,
		CreatedAt: time.Now().UTC(),
		Content:   req.Content,
	}
	data, err := json.Marshal(book)
	if err != nil {
		log.Println(err)
		errorResponse(c, http.StatusInternalServerError, err.Error())
		return
	}
	js := string(data)
	_, err = elasticClient.Index().Index("books").Type("book").Id(book.ID).BodyJson(js).Do(c)
	if err != nil {
		log.Println(err)
		errorResponse(c, http.StatusInternalServerError, err.Error())
		return
	}
	c.Status(http.StatusOK)
}

func searchEndpoint(c *gin.Context) {
	query := c.Query("query")
	if query == "" {
		errorResponse(c, http.StatusBadRequest, "Query not specified")
		return
	}
	skip := 0
	take := 10
	if i, err := strconv.Atoi(c.Query("skip")); err == nil {
		skip = i
	}
	if i, err := strconv.Atoi(c.Query("take")); err == nil {
		take = i
	}
	esQuery := elastic.NewMultiMatchQuery(query, "title", "content").
		Fuzziness("2").
		MinimumShouldMatch("2")
	result, err := elasticClient.Search().
		Index(elasticIndexName).
		Query(esQuery).
		From(skip).Size(take).
		Do(c.Request.Context())
	if err != nil {
		log.Println(err)
		errorResponse(c, http.StatusInternalServerError, err.Error())
		return
	}
	res := SearchResponse{
		Time: fmt.Sprintf("%d", result.TookInMillis),
		Hits: fmt.Sprintf("%d", result.Hits.TotalHits),
	}
	books := make([]Book, 0)
	for _, hit := range result.Hits.Hits {
		var book Book
		json.Unmarshal(*hit.Source, &book)
		books = append(books, book)
	}
	res.Books = books
	c.JSON(http.StatusOK, res)
}

func errorResponse(c *gin.Context, code int, err string) {
	c.JSON(code, gin.H{
		"error": err,
	})
}
