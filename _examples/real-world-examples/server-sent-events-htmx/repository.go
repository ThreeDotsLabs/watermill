package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"time"
)

const migration = `
CREATE TABLE IF NOT EXISTS posts (
	id serial PRIMARY KEY,
	author VARCHAR NOT NULL,
	content TEXT NOT NULL,
	views INT NOT NULL DEFAULT 0,
	reactions JSONB NOT NULL DEFAULT '{}',
	created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

INSERT INTO posts (id, author, content) VALUES
	(1, 'Miłosz', 'Oh, I remember the days when we used to write code in PHP!'),
	(2, 'Robert', 'Back in my days, we used to write code in assembly!')
ON CONFLICT (id) DO NOTHING;
`

func MigrateDB(db *sql.DB) error {
	_, err := db.Exec(migration)
	return err
}

type Repository struct {
	db *sql.DB
}

func NewRepository(db *sql.DB) *Repository {
	return &Repository{
		db: db,
	}
}

func (s *Repository) PostByID(ctx context.Context, id int) (Post, error) {
	row := s.db.QueryRowContext(ctx, `SELECT id, author, content, views, reactions, created_at FROM posts WHERE id = $1`, id)
	post, err := scanPost(row)
	if err != nil {
		return Post{}, err
	}

	return post, nil
}

func (s *Repository) AllPosts(ctx context.Context) ([]Post, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT id, author, content, views, reactions, created_at FROM posts ORDER BY id ASC`)
	if err != nil {
		return nil, err
	}

	var posts []Post
	for rows.Next() {
		post, err := scanPost(rows)
		if err != nil {
			return nil, err
		}
		posts = append(posts, post)
	}

	return posts, nil
}

func (s *Repository) UpdatePost(ctx context.Context, id int, updateFn func(post *Post)) (err error) {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}

	defer func() {
		if err == nil {
			err = tx.Commit()
		} else {
			txErr := tx.Rollback()
			if txErr != nil {
				err = txErr
			}
		}
	}()

	row := s.db.QueryRowContext(ctx, `SELECT id, author, content, views, reactions, created_at FROM posts WHERE id = $1 FOR UPDATE`, id)
	post, err := scanPost(row)
	if err != nil {
		return err
	}

	updateFn(&post)

	reactionsJSON, err := json.Marshal(post.Reactions)
	if err != nil {
		return err
	}

	_, err = tx.ExecContext(ctx, `UPDATE posts SET views = $1, reactions = $2 WHERE id = $3`, post.Views, reactionsJSON, post.ID)
	if err != nil {
		return err
	}

	return nil
}

type scanner interface {
	Scan(dest ...any) error
}

func scanPost(s scanner) (Post, error) {
	var id, postViews int
	var author, content string
	var reactions []byte
	var createdAt time.Time

	err := s.Scan(&id, &author, &content, &postViews, &reactions, &createdAt)
	if err != nil {
		return Post{}, err
	}

	var reactionsMap map[string]int
	err = json.Unmarshal(reactions, &reactionsMap)
	if err != nil {
		return Post{}, err
	}

	return Post{
		ID:        id,
		Author:    author,
		Content:   content,
		CreatedAt: createdAt,
		Views:     postViews,
		Reactions: reactionsMap,
	}, nil
}
