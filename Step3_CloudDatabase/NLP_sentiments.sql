CREATE TABLE `noncloud_hospital_sentiments` (
  `hospital_name` text,
  `address` text,
  `hospital_rating` float DEFAULT NULL,
  `author_name` text,
  `author_id` varchar(64) NOT NULL,
  `review` text,
  `sentiment` varchar(20) DEFAULT NULL,
  PRIMARY KEY (`author_id`)
);

CREATE TABLE `gcp_hospital_sentiment` (
  `hospital_name` text,
  `address` text,
  `hospital_rating` float DEFAULT NULL,
  `author_name` text,
  `author_id` varchar(64) NOT NULL,
  `review` text,
  `sentiment` varchar(20) DEFAULT NULL,
  `sentiment_score` float DEFAULT NULL,
  PRIMARY KEY (`author_id`)
);
