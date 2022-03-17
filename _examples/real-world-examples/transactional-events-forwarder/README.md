# Publishing events in transactions with help of Forwarder component (MySQL to Google Pub/Sub)  

While working with an event-driven application, you may in some point need to store an application state and publish a message
telling the rest of the system about what just happened. As it may look trivial at a first glance, it could become
a bit tricky if we consider what can go wrong in case we won't pay enough attention to details.

## Solution

This example presents a solution to this problem: saving events in transaction along with persisting application state. 
It also compares two other approaches which lack transactional publishing therefore expose application to a risk 
of inconsistency across the system. 

## Requirements

To run this example you will need Docker and docker-compose installed. See installation guide at https://docs.docker.com/compose/install/

## Running

```bash
docker-compose up
```
