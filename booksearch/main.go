package main

import (
	"bufio"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/olivere/elastic"
	"golang.org/x/net/context"
)

const (
	elasticIndexName = "books"
	elasticTypeName  = "book"
	baseUrlTitle     = "http://www.gutenberg.org/files/"
	layout1          = "January 2, 2006"
	layout2          = "January, 2006"
	layout3          = "2006"
)

type Book struct {
	ID         string    `json:"id"`
	Title      string    `json:"title"`
	Author     string    `json:"author"`
	CreatedAt  time.Time `json:"created_at"`
	ReleasedAt time.Time `json:"released_at"`
	Content    string    `json:"content"`
}

type CreateBookRequest struct {
	ID         string    `json:"id"`
	Title      string    `json:"title"`
	Author     string    `json:"author"`
	ReleasedAt time.Time `json:"released_at"`
	Content    string    `json:"content"`
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
	// ticker := time.NewTicker(1 * time.Minute)
	// done := make(chan bool)
	// go func() {
	// 	for {
	// 		select {
	// 		case <-done:
	// 			return
	// 		case <-ticker.C:
	// 			//ticker = time.NewTicker(24 * time.Hour)
	// 			crawlBooks()
	// 		}
	// 	}
	// }()

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

	// for {

	// }
	// ticker.Stop()
	// done <- true
}

func crawlBooks() {
	bulk := elasticClient.Bulk()
	ctx := context.Background()
	var index int
	for index = startIndex; index < startIndex+10; index++ {
		url := baseUrlTitle + strconv.Itoa(index) + "/" + strconv.Itoa(index) + ".txt"
		res, err := http.Get(url)
		if err != nil {
			log.Println("can't get url data")
			return
		}
		defer res.Body.Close()
		title, author, release_date, content := extractData(res.Body)
		if title != "" {
			log.Println("at index:" + strconv.Itoa(index))
			log.Println(title)
			log.Println(author)
			log.Println(release_date)
			rdate := parseAsDate(release_date)
			log.Println(rdate)
			book := Book{
				ID:         strconv.Itoa(index),
				Title:      title,
				Author:     author,
				CreatedAt:  time.Now().UTC(),
				ReleasedAt: rdate,
				Content:    content,
			}
			req := elastic.NewBulkIndexRequest().Index("books").Type("book").Id(strconv.Itoa(index)).Doc(book)
			bulk = bulk.Add(req)
		}
	}
	_, err := bulk.Do(ctx)
	if err != nil {
		log.Println("Bulk insert failed")
		log.Println(err)
	} else {
		log.Println("Bulk insert success")
	}
	startIndex = index
}

func parseAsDate(release_date string) time.Time {
	rdate := strings.Split(release_date, "[")
	rdate2 := strings.TrimSpace(rdate[0])
	t1, err := time.Parse(layout1, rdate2)
	if err != nil {
		t2, err := time.Parse(layout2, rdate2)
		if err != nil {
			t3, err := time.Parse(layout3, rdate2)
			if err != nil {
				return time.Now()
			}
			return t3
		}
		return t2
	}
	return t1
}

func extractData(res io.Reader) (string, string, string, string) {
	var title = ""
	var author = ""
	var rd string
	var progress = 0
	var content strings.Builder
	scanner := bufio.NewScanner(res)
	for scanner.Scan() {
		text := scanner.Text()
		if err := scanner.Err(); err != nil {
			break
		}
		if progress == 1 {
			content.WriteString(text + " ")
		} else if strings.Contains(text, "Title: ") {
			title = strings.TrimPrefix(text, "Title: ")
		} else if strings.Contains(text, "Author: ") {
			author = strings.TrimPrefix(text, "Author: ")
		} else if strings.Contains(text, "*** START") {
			progress = 1
		} else if strings.Contains(text, "*** END") {
			break
		} else if strings.Contains(text, "Release Date: ") {
			rd = strings.TrimPrefix(text, "Release Date: ")
		}
	}
	return title, author, rd, content.String()
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
		ID:         req.ID,
		Title:      req.Title,
		Author:     req.Author,
		CreatedAt:  time.Now().UTC(),
		ReleasedAt: req.ReleasedAt,
		Content:    req.Content,
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
