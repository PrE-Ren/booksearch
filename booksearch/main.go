package main

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"strconv"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/olivere/elastic"
	"github.com/teris-io/shortid"
)

const (
	elasticIndexName = "books"
	elasticTypeName  = "book"
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

type BookResponse struct {
	Title     string    `json:"title"`
	CreatedAt time.Time `json:"created_at"`
	Content   string    `json:"content"`
}

type SearchResponse struct {
	Time  string         `json:"time"`
	Hits  string         `json:"hits"`
	Books []BookResponse `json:"books"`
}

var (
	elasticClient *elastic.Client
)

func main() {
	var err error
	for {
		elasticClient, err = elastic.NewClient(
			elastic.SetURL("http://elasticsearch:9200"),
			elastic.SetSniff(false),
		)
		if err != nil {
			log.Println(err)
			time.Sleep(3 * time.Second)
		} else {
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
}

func getBookEndpoint(c *gin.Context) {
	id := c.Query("id")
	if id == "" {
		errorResponse(c, http.StatusBadRequest, "Id not specified")
		return
	}
	res, err := elasticClient.Get().Index("books").Id(id).Do(c)
	if err != nil {
		log.Println(err)
		errorResponse(c, http.StatusInternalServerError, "Failed to get book")
		return
	}
	c.JSON(http.StatusOK, res)
}

func createBookEndpoint(c *gin.Context) {
	// Parse request
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
		errorResponse(c, http.StatusInternalServerError, "Failed to create book")
		return
	}
	js := string(data)
	_, err = elasticClient.Index().Index("books").BodyJson(js).Do(c)
	if err != nil {
		log.Println(err)
		errorResponse(c, http.StatusInternalServerError, "Failed to create book")
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
		errorResponse(c, http.StatusInternalServerError, "Something went wrong")
		return
	}
	res := SearchResponse{
		Time: fmt.Sprintf("%d", result.TookInMillis),
		Hits: fmt.Sprintf("%d", result.Hits.TotalHits),
	}
	books := make([]BookResponse, 0)
	for _, hit := range result.Hits.Hits {
		var book BookResponse
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
