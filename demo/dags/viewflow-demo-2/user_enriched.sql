/*
---
owner: data@datacamp.com
description: A table with enriched information of users
fields:
  user_id: The user id
  xp: The user amount of XP
  last_course_completed_at: When was the last course completed by a user
  last_course_completed: Name of the latest completed course by a user
  number_courses_completed: Number of completed courses by a user 
schema: viewflow_demo
connection_id: postgres_demo
---
*/

WITH users_last_course AS (
	SELECT 
		uc.user_id,
		uxp.xp,
		RANK() OVER (PARTITION BY uc.user_id ORDER BY completed_at DESC) AS rn,
		completed_at AS last_course_completed_at,
		c.title AS last_course_completed
	FROM viewflow_raw.user_course uc 
	LEFT JOIN viewflow_raw.courses c ON c.id = uc.course_id 
	LEFT JOIN viewflow_demo.user_xp uxp ON uxp.user_id = uc.user_id
), user_number_courses_completed AS (
	SELECT 
		DISTINCT
		uc.user_id,
		COUNT(uc.id) OVER (PARTITION BY uc.user_id) AS number_courses_completed
	FROM viewflow_raw.user_course uc  
	WHERE completed_at IS NOT NULL
) SELECT 
	ulc.user_id, 
	xp, 
	last_course_completed_at, 
	last_course_completed, 
	number_courses_completed  
FROM users_last_course  ulc
LEFT JOIN user_number_courses_completed uncc ON ulc.user_id = uncc.user_id 
WHERE rn=1