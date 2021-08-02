/*
---
owner: data@datacamp.com
type: IncrementalPostgresOperator
description: For all users, list the email address and notification mode of the category blog.
fields:
  user_id: The user ID
  notification_mode: The detailed mode for which notifications to receive
  email: Email address of the user
schema: viewflow_demo
connection_id: postgres_demo
primary_key: [user_id]
time_parameters:
  initial:
    min_time: '''2020-01-01 12:00:00'''
  update:
    min_time: (SELECT max(updated_at) FROM viewflow_demo.emails_blog)
---
*/

SELECT user_id, notification_mode, email, updated_at
FROM
  viewflow_raw.notifications n
  INNER JOIN viewflow_raw.users u
  ON n.user_id = u.id
WHERE
  category = 'blog' AND
  updated_at >= {{min_time}}
