/*
---
owner: data@datacamp.com
description: Provide the total amount of XP for each user
fields:
  user_id: The user id
  xp: The sum of XP
schema: viewflow_demo
connection_id: postgres_demo
---
*/

SELECT 
	DISTINCT
	u.id AS user_id,
	SUM(xp) OVER (PARTITION BY u.id) AS xp
FROM 
  viewflow_raw.users u
LEFT JOIN viewflow_raw.user_course uc ON uc.user_id = u.id
LEFT JOIN viewflow_raw.courses c ON c.id  = uc.course_id
ORDER BY 
  user_id