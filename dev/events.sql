CREATE TABLE IF NOT EXISTS messages (
  offset BIGINT AUTO_INCREMENT,
  uuid BINARY(16) NOT NULL,
  created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  payload JSON NOT NULL,
  metadata JSON NOT NULL,
  topic VARCHAR(255) NOT NULL,
  PRIMARY KEY (uuid),
  UNIQUE(offset)
);

CREATE INDEX CLUSTERED ON messages(offset);

CREATE TABLE IF NOT EXISTS offsets_acked (
  offset BIGINT NOT NULL,
  consumer_group VARCHAR(255) NOT NULL,
  PRIMARY KEY(consumer_group),
  FOREIGN KEY (offset) REFERENCES messages(offset)
);
