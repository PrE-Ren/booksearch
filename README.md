About Booksearch 

-DB
Use Elasticsearch DB by index ‘books’
Index ‘books’ contains many struct ‘book’s
Book has 6 fields (id: int, title: string, author: string, released_at: time.Time, created_at: time.Time, content: string) , use string(id) as ID in Elasticsearch.

-Server & API
http://localhost:8080/books?id=abc
can GET book which has ID ‘abc’
or DELETE book which has ID ‘abc’
http://localhost:8080/books
can POST Book as JSON
or PUT Book as JSON (ID as Book’s id)
http://localhost:8080/search?query=abc&field=content&sort=score 
can GET search book which matches query ‘abc’ from field ‘content’ (can use ‘title’ or ‘author’) and sort by ‘score’ (can use ‘time_new’ or ‘time_old’, ‘alphabet’)
Default parameter for field is content, sort is score, query cannot be blank.
Sort by ‘time_new’ means by release date descending order, ‘time_old’ as ascending, ‘alphbet’ as title alphabetical order.  

