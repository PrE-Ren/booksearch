package main

import (
	"bufio"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"sort"
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

type SearchBook struct {
	ID         string    `json:"id"`
	Title      string    `json:"title"`
	Author     string    `json:"author"`
	ReleasedAt time.Time `json:"released_at"`
	Score      float64   `json:"score"`
	// Highlight  []string  `json:"highlight"`
}

type SearchResponse struct {
	Books []SearchBook `json:"books"`
}

var (
	elasticClient *elastic.Client
	startIndex    int
)

func main() {
	f, err := os.OpenFile("data/startindex.txt", os.O_RDWR|os.O_CREATE, 0666)
	if err != nil {
		log.Println(err)
		return
	} else {
		_, err = fmt.Fscanf(f, "%d\n", &startIndex)
		if err == io.EOF {
			log.Println(err)
			startIndex = 1
		}
	}
	f.Close()

	ticker := time.NewTicker(1 * time.Minute)
	if startIndex > 4000 {
		ticker = time.NewTicker(24 * time.Hour)
	}
	done := make(chan bool)
	go func() {
		for {
			select {
			case <-done:
				return
			case <-ticker.C:
				if startIndex > 4000 {
					ticker = time.NewTicker(24 * time.Hour)
				}
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
			break
		}
	}

	r := gin.Default()
	r.PUT("/books", putBookEndpoint)
	r.DELETE("/books", deleteBookEndpoint)
	r.POST("/books", postBookEndpoint)
	r.GET("/books", getBookEndpoint)
	r.GET("/search", searchEndpoint)
	if err = r.Run(":8080"); err != nil {
		log.Fatal(err)
	}

	for {

	}
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
		f, err := os.OpenFile("data/startindex.txt", os.O_RDWR, 0644)
		if err != nil {
			log.Println("error in open file")
			return
		}
		_, err = f.WriteString(strconv.Itoa(index))
		if err != nil {
			log.Println("error writing file")
			f.Close()
			return
		}
		f.Sync()
		f.Close()
		startIndex = index
	}
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

func postBookEndpoint(c *gin.Context) {
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

func putBookEndpoint(c *gin.Context) {
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
	_, err := elasticClient.Update().Index("books").Type("book").Id(book.ID).Doc(book).Do(c)
	if err != nil {
		log.Println(err)
		errorResponse(c, http.StatusInternalServerError, err.Error())
		return
	}
	c.Status(http.StatusOK)
}

func deleteBookEndpoint(c *gin.Context) {
	id := c.Query("id")
	if id == "" {
		errorResponse(c, http.StatusBadRequest, "Id not specified")
		return
	}
	res, err := elasticClient.Delete().Index("books").Type("book").Id(id).Do(c)
	if err != nil {
		log.Println(err)
		errorResponse(c, http.StatusInternalServerError, err.Error())
		return
	}
	c.JSON(http.StatusOK, res)
}

func searchEndpoint(c *gin.Context) {
	query := c.Query("query")
	if query == "" {
		errorResponse(c, http.StatusBadRequest, "Query not specified")
		return
	}
	sort_by := c.Query("sort")
	field := c.Query("field")
	if field == "" {
		field = "content"
	}
	skip := 0
	take := 1000
	take_more := 30
	terms := strings.Split(query, " ")
	clause := make([]map[string]interface{}, 0)
	for i := 0; i < len(terms); i++ {
		clause = append(clause, map[string]interface{}{
			"span_multi": map[string]interface{}{
				"match": map[string]interface{}{
					"fuzzy": map[string]interface{}{
						field: map[string]interface{}{
							"fuzziness": strconv.Itoa(getMaxFuzzy(len(terms[i]))),
							"value":     terms[i],
						},
					},
				},
			},
		})
	}

	esQuery := map[string]interface{}{
		"span_near": map[string]interface{}{
			"clauses":  clause,
			"slop":     1,
			"in_order": "true",
		},
	}

	queryJson, err := json.Marshal(esQuery)
	if err != nil {
		log.Println(err)
		errorResponse(c, http.StatusInternalServerError, err.Error())
		return
	}

	highlighter := elastic.NewHighlight().HighlighterType("plain").Field("content")

	result, err := elasticClient.Search().
		Index(elasticIndexName).
		Query(elastic.RawStringQuery(string(queryJson))).
		From(skip).
		Size(take).TrackScores(false).
		Highlight(highlighter).
		Do(c.Request.Context())

	if err != nil {
		log.Println(err)
		errorResponse(c, http.StatusInternalServerError, err.Error())
		return
	}

	var res SearchResponse

	books := make([]SearchBook, 0)
	for _, hit := range result.Hits.Hits {
		var book SearchBook
		json.Unmarshal(*hit.Source, &book)
		book.Score = getScore(hit.Highlight["content"], terms, true)
		books = append(books, book)
	}

	if len(terms) > 1 && len(books) < 30 {
		for i := 0; i < len(terms); i++ {
			clause := make([]map[string]interface{}, 0)
			for j := 0; j < len(terms); j++ {
				if j != i {
					clause = append(clause, map[string]interface{}{
						"span_multi": map[string]interface{}{
							"match": map[string]interface{}{
								"fuzzy": map[string]interface{}{
									field: map[string]interface{}{
										"fuzziness": strconv.Itoa(getMaxFuzzy(len(terms[i]))),
										"value":     terms[i],
									},
								},
							},
						},
					})
				}
			}

			esQuery := map[string]interface{}{
				"span_near": map[string]interface{}{
					"clauses":  clause,
					"slop":     1,
					"in_order": "true",
				},
			}

			queryJson, err := json.Marshal(esQuery)
			if err != nil {
				log.Println(err)
				errorResponse(c, http.StatusInternalServerError, err.Error())
				return
			}

			result, err := elasticClient.Search().
				Index(elasticIndexName).
				Query(elastic.RawStringQuery(string(queryJson))).
				From(skip).
				Size(take_more).TrackScores(false).
				Highlight(highlighter).
				Do(c.Request.Context())

			if err != nil {
				log.Println(err)
				errorResponse(c, http.StatusInternalServerError, err.Error())
				return
			}

			for _, hit := range result.Hits.Hits {
				var book SearchBook
				json.Unmarshal(*hit.Source, &book)
				if i == 0 {
					book.Score = getScore(hit.Highlight["content"], terms[1:], false)
				} else if i == (len(terms) - 1) {
					book.Score = getScore(hit.Highlight["content"], terms[:i], false)
				} else {
					tmp_terms := make([]string, 0)
					tmp_terms = append(tmp_terms, terms[:i]...)
					tmp_terms = append(tmp_terms, terms[i+1:]...)
					book.Score = getScore(hit.Highlight["content"], tmp_terms, false)
				}
				books = append(books, book)
			}
		}
	}
	sorted_books := sortByField("score", removeDuplicates(books))
	if len(sorted_books) > 30 {
		res.Books = sorted_books[:30]
	} else {
		res.Books = sorted_books
	}

	if sort_by != "" {
		field_sorted_books := sortByField(sort_by, sorted_books)
		if len(field_sorted_books) > 10 {
			res.Books = field_sorted_books[:10]
		} else {
			res.Books = field_sorted_books
		}
	} else {
		if len(sorted_books) > 10 {
			res.Books = sorted_books[:10]
		} else {
			res.Books = sorted_books
		}
	}

	c.JSON(http.StatusOK, res)
}

func getScore(input []string, terms []string, supplement bool) float64 {
	count := len(terms)
	max_terms := count
	if !supplement {
		max_terms += 1
	}
	max_score := max_terms*max_terms + 1
	current_max := 0.0

	if supplement {
		current_max += float64(max_terms*max_terms + 2)
		max_score += max_terms*max_terms + 1
	}

	for _, sentence := range input {
		words := strings.Split(sentence, " ")
		word_num := 0
		gaps := 0
		total_fuzzy := 0.0
		is_middle := false
		for _, word := range words {
			if word == "" {
				continue
			}
			parts := strings.Split(word, "\u003cem\u003e")
			if len(parts) == 1 {
				if is_middle {
					gaps += 1
				}
				continue
			} else if len(parts) == 2 {
				real_word := strings.Split(parts[1], "\u003c/em\u003e")
				max_fuzzy := getMaxFuzzy(len(terms[word_num]))
				if max_fuzzy != 0 {
					total_fuzzy += (float64(getFuzzyCount(real_word[0], terms[word_num])) / float64(max_fuzzy))
				}
				word_num += 1
				if word_num == count {
					is_middle = false
					word_num = 0
					break
				} else {
					is_middle = true
				}
			}
		}

		if !is_middle {
			score := float64(max_score) - float64(gaps)*float64(max_terms) - total_fuzzy
			if current_max < score {
				current_max = score
				if current_max == float64(max_score) {
					return current_max
				}
			}
		}
	}

	return current_max
}

func getMaxFuzzy(input int) int {
	if input >= 8 {
		return 2
	} else if input >= 4 {
		return 1
	} else {
		return 0
	}
}

func getFuzzyCount(s1 string, s2 string) int {
	input := strings.ToLower(s1)
	query := strings.ToLower(s2)
	diff_len := len(input) - len(query)
	if diff_len == 1 {
		for i := 0; i < len(input); i++ {
			if i == 0 {
				if query == input[1:] {
					return 1
				}
			} else {
				if query == (input[0:i] + input[i+1:]) {
					return 1
				}
			}
		}
		return 2
	}

	if diff_len == -1 {
		for i := 0; i < len(query); i++ {
			if i == 0 {
				if input == query[1:] {
					return 1
				}
			} else {
				if input == (query[0:i] + query[i+1:]) {
					return 1
				}
			}
		}
		return 2
	}

	if diff_len == 0 {
		if query == input {
			return 0
		}
		for i := 0; i < len(query); i++ {
			if i == 0 {
				if input[1:] == query[1:] {
					return 1
				} else if (input[0] == query[1]) && (query[0] == input[1]) {
					if len(input) == 2 {
						return 1
					} else {
						if input[2:] == query[2:] {
							return 1
						}
					}
				}
			} else if i == (len(input) - 1) {
				if input[0:i] == query[0:i] {
					return 1
				}
			} else {
				if (input[0:i] + input[i+1:]) == (query[0:i] + query[i+1:]) {
					return 1
				} else if (input[i] == query[i+1]) && (query[i] == input[i+1]) {
					if i == (len(input) - 2) {
						if input[0:i] == query[0:i] {
							return 1
						}
					} else if (input[0:i] + input[i+2:]) == (query[0:i] + query[i+2:]) {
						return 1
					}
				}
			}
		}
		return 2
	}
	return 2
}

func removeDuplicates(bookList []SearchBook) []SearchBook {
	filteredBooks := make([]SearchBook, 0)
	existingId := make(map[string]bool, 0)
	for i := 0; i < len(bookList); i++ {
		_, ok := existingId[bookList[i].ID]
		if !ok {
			existingId[bookList[i].ID] = true
			filteredBooks = append(filteredBooks, bookList[i])
		}
	}
	return filteredBooks
}

func sortByField(field string, list []SearchBook) []SearchBook {
	new_list := make([]SearchBook, 0)
	new_list = append(new_list, list...)
	if field == "score" {
		sort.SliceStable(new_list, func(i, j int) bool {
			return new_list[i].Score > new_list[j].Score
		})
		return new_list
	} else if field == "time_new" {
		sort.SliceStable(new_list, func(i, j int) bool {
			return new_list[i].ReleasedAt.After(new_list[j].ReleasedAt)
		})
		return new_list
	} else if field == "time_old" {
		sort.SliceStable(new_list, func(i, j int) bool {
			return new_list[i].ReleasedAt.Before(new_list[j].ReleasedAt)
		})
		return new_list
	} else if field == "alphabet" {
		sort.SliceStable(new_list, func(i, j int) bool {
			return new_list[i].Title < new_list[j].Title
		})
	}

	return new_list
}

func errorResponse(c *gin.Context, code int, err string) {
	c.JSON(code, gin.H{
		"error": err,
	})
}
