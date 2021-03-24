CREATE SCHEMA IF NOT EXISTS viewflow_demo;
CREATE SCHEMA IF NOT EXISTS viewflow_raw;

DROP TABLE IF EXISTS viewflow_raw.users;
CREATE TABLE viewflow_raw.users (
  id SERIAL,
  email VARCHAR,
  password VARCHAR
);

INSERT INTO
  viewflow_raw.users (email, password)
VALUES
  ('user_1@datacamp.com', 'c23b2ed66eedb321c5bcfb5e3724b978'),
  ('user_2@datacamp.com', '3b46afa69314bf5a2f885a532cfab7c4'),
  ('user_3@datacamp.com', 'bb86c291743c3edcf6c76e4ff69f974f'),
  ('user_4@datacamp.com', '5bd81d5fecc184e59cad0931fd66e95f'),
  ('user_5@datacamp.com', '699bf56f37394791f36f154ee3aa56a6'),
  ('user_6@datacamp.com', 'b3b34de71d480eeb5c1ed28567cc4316'),
  ('user_7@datacamp.com', '7cc5708de924188a02d52e9d0853800c'),
  ('user_8@datacamp.com', '3a8f84f28514a9f0c6d9ce7a539cce23'),
  ('user_9@datacamp.com', '78d1860ed582f28f7f2b6d77cb3345df'),
  ('user_10@datacamp.com', 'ef40ccd80c57866419fc8da1e96dfb56');



DROP TABLE IF EXISTS viewflow_raw.courses;
CREATE TABLE viewflow_raw.courses (
  id SERIAL,
  title VARCHAR,
  xp integer
);
INSERT INTO
  viewflow_raw.courses (title, xp)
VALUES
('Introduction to Python', 100),
('Introduction to R', 100),
('Machine learning with Python', 250),
('Introduction to tidyverse', 120),
('Plotting with ggplot', 180);

DROP TABLE IF EXISTS viewflow_raw.user_course;
CREATE TABLE viewflow_raw.user_course (
  id SERIAL,
  user_id integer,
  course_id integer,
  started_at timestamp,
  completed_at timestamp
);
INSERT INTO
  viewflow_raw.user_course (user_id, course_id, started_at, completed_at)
VALUES
(1, 1, '2021-01-01 10:00:00', '2021-01-01 14:00:00'),
(1, 2, '2021-01-02 12:00:00', '2021-01-03 09:00:00'),
(1, 3, '2021-01-03 15:00:00', '2021-01-03 19:00:00'),
(1, 4, '2021-01-04 08:00:00', '2021-01-05 11:00:00'),
(1, 5, '2021-01-05 13:00:00', '2021-01-05 18:00:00'),
(2, 1, '2021-02-01 10:00:00', '2021-02-01 14:00:00'),
(2, 2, '2021-01-02 08:00:00', '2021-01-02 12:00:00'),
(3, 4, '2021-02-15 11:00:00', '2021-02-15 17:00:00'),
(3, 5, '2021-02-16 10:00:00', '2021-02-15 14:00:00'),
(3, 3, '2021-02-17 17:00:00', '2021-02-17 22:00:00'),
(4, 1, '2021-03-01 10:00:00', '2021-03-01 14:00:00'),
(4, 2, '2021-03-02 10:00:00', '2021-03-02 14:00:00'),
(4, 4, '2021-03-03 10:00:00', '2021-03-03 14:00:00'),
(4, 5, '2021-03-04 10:00:00', '2021-03-04 14:00:00'),
(5, 2, '2021-03-05 10:00:00', '2021-03-05 14:00:00'),
(5, 3, '2021-03-06 10:00:00', '2021-03-06 14:00:00'),
(5, 4, '2021-03-07 10:00:00', '2021-03-07 14:00:00'),
(5, 5, '2021-03-08 10:00:00', '2021-03-08 14:00:00'),
(6, 3, '2021-03-09 10:00:00', '2021-03-10 14:00:00'),
(6, 5, '2021-03-10 10:00:00', '2021-03-10 18:00:00'),
(7, 4, '2021-02-01 10:00:00', '2021-01-02 14:00:00'),
(7, 5, '2021-02-04 10:00:00', '2021-01-04 14:00:00'),
(8, 1, '2021-01-19 10:00:00', '2021-01-19 14:00:00'),
(8, 5, '2021-01-21 10:00:00', '2021-01-21 14:00:00'),
(9, 1, '2021-02-15 10:00:00', '2021-02-16 14:00:00'),
(10, 1, '2021-03-15 10:00:00', '2021-03-15 14:00:00'),
(10, 3, '2021-03-16 10:00:00', '2021-03-16 14:00:00');