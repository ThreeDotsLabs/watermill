# Example Golang CQRS application

This application is using [Watermill CQRS](http://watermill.io/docs/cqrs) component.

Detailed documentation for CQRS can be found in Watermill's docs: [http://watermill.io/docs/cqrs#usage](http://watermill.io/docs/cqrs).

![CQRS Event Storming](https://threedots.tech/watermill-io/cqrs-example-storming.png)

```mermaid
sequenceDiagram
    participant M as Main
    participant CB as CommandBus
    participant BRH as BookRoomHandler
    participant EB as EventBus
    participant OBRB as OrderBeerOnRoomBooked
    participant OBH as OrderBeerHandler
    participant BFR as BookingsFinancialReport

    Note over M,BFR: Commands use AMQP queue, Events use AMQP pub/sub
    
    M->>CB: Send(BookRoom)<br/>topic: commands.BookRoom
    CB->>BRH: Handle(BookRoom)
    
    BRH->>EB: Publish(RoomBooked)<br/>topic: events.RoomBooked
    
    par Process RoomBooked Event
        EB->>OBRB: Handle(RoomBooked)
        OBRB->>CB: Send(OrderBeer)<br/>topic: commands.OrderBeer
        CB->>OBH: Handle(OrderBeer)
        OBH->>EB: Publish(BeerOrdered)<br/>topic: events.BeerOrdered
        
        EB->>BFR: Handle(RoomBooked)
        Note over BFR: Updates financial report
    end
```


## Running

```bash
docker-compose up
```
