package main

import (
	"context"
	"time"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.mongodb.org/mongo-driver/mongo/readpref"
)

const collectionName = "feeds"

type FeedsStorage struct {
	collection *mongo.Collection
}

func NewFeedsStorage() FeedsStorage {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	client, err := mongo.Connect(ctx, options.Client().ApplyURI("mongodb://root:password@mongo:27017"))
	if err != nil {
		panic(err)
	}

	err = client.Ping(ctx, readpref.Primary())
	if err != nil {
		panic(err)
	}

	db := client.Database("example")
	names, err := db.ListCollectionNames(ctx, bson.M{})
	if err != nil {
		panic(err)
	}

	found := false
	for _, n := range names {
		if n == collectionName {
			found = true
			break
		}
	}

	if !found {
		err := db.CreateCollection(ctx, collectionName)
		if err != nil {
			panic(err)
		}
	}

	return FeedsStorage{
		collection: db.Collection(collectionName),
	}
}

func (s FeedsStorage) Add(ctx context.Context, name string) error {
	feed := Feed{
		Name:  name,
		Posts: []Post{},
	}

	_, err := s.collection.InsertOne(ctx, feed)
	if err != nil {
		if !isDuplicateError(err) {
			return err
		}
	}

	return nil
}

func (s FeedsStorage) All(ctx context.Context) ([]Feed, error) {
	cursor, err := s.collection.Find(ctx, bson.M{})
	if err != nil {
		return nil, err
	}

	var feeds []Feed

	err = cursor.All(ctx, &feeds)
	if err != nil {
		return nil, err
	}

	return feeds, nil
}

func (s FeedsStorage) ByName(ctx context.Context, name string) (Feed, error) {
	filter := bson.M{
		"_id": name,
	}

	var feed Feed
	err := s.collection.FindOne(ctx, filter).Decode(&feed)
	if err != nil {
		return Feed{}, err
	}

	return feed, nil
}

func (s FeedsStorage) AppendPost(ctx context.Context, post Post) error {
	return s.appendPostIfNotPresent(ctx, post)
}

func (s FeedsStorage) UpdatePost(ctx context.Context, post Post) error {
	err := s.updatePostIfPresent(ctx, post)
	if err != nil {
		return err
	}

	err = s.appendPostIfNotPresent(ctx, post)
	if err != nil {
		return err
	}

	err = s.removePostIfNotInFeed(ctx, post)
	if err != nil {
		return err
	}

	return nil
}

func (s FeedsStorage) updatePostIfPresent(ctx context.Context, post Post) error {
	filter := bson.M{
		"_id": bson.M{
			"$in": post.Tags,
		},
		"posts.id": post.ID,
	}

	update := bson.M{
		"$set": bson.M{
			"posts.$": post,
		},
	}

	_, err := s.collection.UpdateMany(ctx, filter, update)
	return err
}

func (s FeedsStorage) appendPostIfNotPresent(ctx context.Context, post Post) error {
	filter := bson.M{
		"_id": bson.M{
			"$in": post.Tags,
		},
		"posts.id": bson.M{
			"$ne": post.ID,
		},
	}

	update := bson.M{
		"$push": bson.M{
			"posts": bson.M{
				"$each":     bson.A{post},
				"$position": 0,
			},
		},
	}

	_, err := s.collection.UpdateMany(ctx, filter, update)
	return err
}

func (s FeedsStorage) removePostIfNotInFeed(ctx context.Context, post Post) error {
	filter := bson.M{
		"_id": bson.M{
			"$nin": post.Tags,
		},
		"posts.id": post.ID,
	}

	update := bson.M{
		"$pull": bson.M{
			"posts": bson.M{
				"id": post.ID,
			},
		},
	}

	_, err := s.collection.UpdateMany(ctx, filter, update)
	if err != nil {
		return err
	}

	return nil
}

func isDuplicateError(err error) bool {
	mErr, ok := err.(mongo.WriteException)
	if !ok {
		return false
	}

	return mErr.WriteErrors[0].Code == 11000
}
