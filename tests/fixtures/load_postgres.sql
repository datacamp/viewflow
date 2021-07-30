CREATE SCHEMA IF NOT EXISTS viewflow;

DROP TABLE IF EXISTS viewflow.users;

CREATE TABLE viewflow.users (
  user_id SERIAL,
  email VARCHAR,
  password VARCHAR
);

INSERT INTO
  viewflow.users (email, password)
VALUES
  ('test1@datacamp.com', 'testtest1'),
  ('test2@datacamp.com', 'testtest2'),
  ('test3@datacamp.com', 'testtest3'),
  ('test4@datacamp.com', 'testtest4'),
  ('test5@datacamp.com', 'testtest5'),
  ('test6@datacamp.com', 'testtest6'),
  ('test7@datacamp.com', 'testtest7'),
  ('test8@datacamp.com', 'testtest8');


DROP TABLE IF EXISTS viewflow.notifications;
CREATE TABLE viewflow.notifications (
  user_id INTEGER,
  category VARCHAR,
  notification_mode VARCHAR,
  updated_at TIMESTAMP,
  PRIMARY KEY (user_id, category)
);
INSERT INTO
  viewflow.notifications (user_id, category, notification_mode, updated_at)
VALUES
  (1, 'daily',       'off',       '2021-12-01 12:00:00'),
  (1, 'recommended', 'off',       '2021-12-01 12:00:00'),
  (1, 'blog',        'selection', '2021-12-01 12:00:00'),
  (2, 'daily',       'all',       '2022-11-01 12:00:00'),
  (2, 'recommended', 'off',       '2022-11-01 12:00:00'),
  (2, 'blog',        'all',       '2022-11-01 12:00:00'),
  (3, 'daily',       'selection', '2023-10-01 12:00:00'),
  (3, 'recommended', 'selection', '2023-10-01 12:00:00'),
  (3, 'blog',        'all',       '2023-10-01 12:00:00');
