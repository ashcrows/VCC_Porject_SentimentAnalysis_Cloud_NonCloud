CREATE TABLE `hospital_lists_ingestion`
  (
     `name`               TEXT,
     `business_status`    VARCHAR(100) DEFAULT NULL,
     `address`            TEXT,
     `latitude`           DOUBLE DEFAULT NULL,
     `longitude`          DOUBLE DEFAULT NULL,
     `open_now`           VARCHAR(10) DEFAULT NULL,
     `rating`             FLOAT DEFAULT NULL,
     `user_ratings_total` INT DEFAULT NULL,
     `types`              TEXT,
     `place_id`           TEXT,
     `loaddatetime`       DATETIME DEFAULT NULL
  )
engine=innodb
DEFAULT charset=utf8mb4
COLLATE=utf8mb4_0900_ai_ci;

CREATE TABLE `hospitals_reviews_ingestion`
  (
     `hospital_name`             TEXT,
     `address`                   TEXT,
     `place_id`                  VARCHAR(100) DEFAULT NULL,
     `hospital_rating`           FLOAT DEFAULT NULL,
     `author_name`               TEXT,
     `author_id`                 VARCHAR(64) DEFAULT NULL,
     `author_url`                TEXT,
     `language`                  VARCHAR(10) DEFAULT NULL,
     `original_language`         VARCHAR(10) DEFAULT NULL,
     `profile_photo_url`         TEXT,
     `review_rating`             INT DEFAULT NULL,
     `relative_time_description` VARCHAR(100) DEFAULT NULL,
     `review`                    TEXT,
     `time`                      DATETIME DEFAULT NULL,
     `translated`                TINYINT DEFAULT NULL,
     `loaddatetime`              DATETIME DEFAULT NULL,
     `sentiments`                TEXT
  )
engine=innodb
DEFAULT charset=utf8mb4
COLLATE=utf8mb4_0900_ai_ci;