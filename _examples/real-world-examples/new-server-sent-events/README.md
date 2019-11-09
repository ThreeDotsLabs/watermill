# Server Sent Events

Aggregate: post
Read model: posts feed

- adapters
  - memory repository for aggregate
  - memory repository for read model
- app
  - command
    - update aggregate & publish event
    - update read model reacting to event
  - query
    - return read model
- ports
  - http
    - return read models
  - pubsub
    - read events
      - PostCreated
        - Update Feed
      - PostUpdated
        - Update Feed
      - FeedUpdated
        - Push changes to http ports via channel
      
