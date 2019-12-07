# Server Sent Events

This example is a Twitter-like web application featuring SSE to achieve real-time refreshing.

## How it works

* Posts can be created and updated
* Posts can contain tags
* Each tag has its own feed that contains all posts from that tag
* Posts are kept in MySQL
* Feeds are updated asynchronously and are kept as a read model in MongoDB
* NATS is used as a Pub/Sub

## Event handlers

* PostCreated
    * Adds the post to all feeds with tags present in the post
* FeedUpdated
    * Pushes update to all clients currently visiting the feed page
* PostUpdated
    * Pushes update to all clients currently visiting the post page
    * Updates post in all feeds with tags present in the post
        * a) For existing tags, the post content will be updated in the tag
        * b) If a new tag has been added, the post will be added to the tag's feed
        * c) If a tag has been deleted, the post will be removed from the tag's feed
