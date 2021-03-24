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

DROP TABLE IF EXISTS viewflow.incremental_users;

CREATE TABLE viewflow.incremental_users (
  user_id SERIAL,
  email VARCHAR,
  password VARCHAR
);

INSERT INTO
  viewflow.incremental_users (email, password)
VALUES
  ('test_incremental1@datacamp.com', 'testtest1'),
  ('test_incremental2@datacamp.com', 'testtest2'),
  ('test_incremental3@datacamp.com', 'testtest3'),
  ('test_incremental4@datacamp.com', 'testtest4'),
  ('test_incremental5@datacamp.com', 'testtest5'),
  ('test_incremental6@datacamp.com', 'testtest6'),
  ('test_incremental7@datacamp.com', 'testtest7'),
  ('test_incremental8@datacamp.com', 'testtest8');