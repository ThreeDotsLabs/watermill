CREATE TABLE IF NOT EXISTS events (
  idx BIGINT AUTO_INCREMENT,
  uuid BINARY(16) NOT NULL,
  created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  payload JSON NOT NULL,
  metadata JSON NOT NULL,
  topic VARCHAR(255) NOT NULL,
  PRIMARY KEY (idx),
  UNIQUE(uuid)
);

CREATE TABLE IF NOT EXISTS subscriber_offsets (
  topic VARCHAR(255) NOT NULL,
  offset BIGINT NOT NULL,
  consumer_group VARCHAR(255) NOT NULL,
  PRIMARY KEY(topic, consumer_group)
);

CREATE TABLE IF NOT EXISTS processed_messages (
  uuid BINARY(16) NOT NULL,
  consumer_group VARCHAR(255) NOT NULL,
  PRIMARY KEY(uuid, consumer_group)
);