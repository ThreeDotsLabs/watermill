-- todo - move it somewhere

CREATE TABLE `events` (
    `event_no` BIGINT NOT NULL AUTO_INCREMENT,
    `event_id` BINARY(16) NOT NULL,
    `event_name` VARCHAR(64) NOT NULL,
    `event_payload` JSON NOT NULL,
    `event_occurred_on` TIMESTAMP NOT NULL,
    `aggregate_version` INT UNSIGNED NOT NULL,
    `aggregate_id` BINARY(16) NOT NULL,
    `aggregate_type` VARCHAR(128) NOT NULL,
    PRIMARY KEY (`event_no`),
    UNIQUE KEY (`event_id`),
    UNIQUE KEY `ix_unique_event` (`aggregate_type`, `aggregate_id`, `aggregate_version`)
);