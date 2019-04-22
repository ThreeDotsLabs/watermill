CREATE TABLE IF NOT EXISTS messages (
  offset BIGINT AUTO_INCREMENT,
  uuid BINARY(16) NOT NULL,
  created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  payload JSON NULL,
  metadata JSON NULL,
  topic VARCHAR(255) NOT NULL,
  PRIMARY KEY (uuid),
  UNIQUE(offset)
);

CREATE TABLE IF NOT EXISTS offsets_acked (
  offset BIGINT NOT NULL,
  consumer_group VARCHAR(255) NOT NULL,
  PRIMARY KEY(consumer_group),
  FOREIGN KEY (offset) REFERENCES messages(offset)
);
