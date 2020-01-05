package main

import (
	"context"
	"database/sql"

	"github.com/go-sql-driver/mysql"
)

type PostsStorage struct {
	db *sql.DB
}

func NewPostsStorage() PostsStorage {
	conf := mysql.NewConfig()
	conf.Net = "tcp"
	conf.User = "root"
	conf.Addr = "mysql"
	conf.DBName = "example"

	db, err := sql.Open("mysql", conf.FormatDSN())
	if err != nil {
		panic(err)
	}

	err = db.Ping()
	if err != nil {
		panic(err)
	}

	return PostsStorage{
		db: db,
	}
}

func (s PostsStorage) ByID(ctx context.Context, id string) (Post, error) {
	query := "SELECT title, content, author FROM posts WHERE id=?"
	row := s.db.QueryRowContext(ctx, query, id)

	var title, content, author string
	err := row.Scan(&title, &content, &author)
	if err != nil {
		return Post{}, err
	}

	return NewPost(id, title, content, author), nil
}

func (s PostsStorage) Add(ctx context.Context, post Post) error {
	query := "INSERT INTO posts (id, title, content, author) VALUES (?, ?, ?, ?)"
	_, err := s.db.ExecContext(ctx, query, post.ID, post.Title, post.Content, post.Author)
	return err
}

func (s PostsStorage) Update(ctx context.Context, post Post) error {
	query := "UPDATE posts SET title=?, content=?, author=? WHERE id=?"
	_, err := s.db.ExecContext(ctx, query, post.Title, post.Content, post.Author, post.ID)
	return err
}
