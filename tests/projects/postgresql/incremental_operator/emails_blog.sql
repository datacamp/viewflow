/*
---
owner: engineering@datacamp.com
type: IncrementalPostgresOperator
description: For all users, list the email address and notification mode of the category blog.
fields:
  user_id: The user ID
  notification_mode: The detailed mode for which notifications to receive
  email: Email address of the user
schema: viewflow
connection_id: postgres_viewflow
time_parameters:
  initial:
    min_time: '''2020-01-01 12:00:00'''
    max_time: '''2021-12-31 12:00:00'''
  update:
    min_time: (SELECT max(updated_at) FROM viewflow.emails_blog)
    max_time: (SELECT (max(updated_at) + interval '1 day' * 365) FROM viewflow.emails_blog)
primary_key: [user_id]
---
*/

SELECT u.user_id, notification_mode, email, updated_at
FROM
  viewflow.users u
  INNER JOIN viewflow.notifications n
  ON n.user_id = u.user_id
WHERE
  category = 'blog' AND
  updated_at >= {{min_time}} AND
  updated_at < {{max_time}}

