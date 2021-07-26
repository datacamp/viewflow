/*
---
owner: data@datacamp.com
description: Enriched the course table
fields:
  course_id: The course id
  course_title: The course title
  number_of_completions: The number of total completions
schema: viewflow_demo
connection_id: postgres_demo
---
*/

SELECT 
	DISTINCT
	course_id,
  title AS course_title,
	COUNT(course_id) OVER (PARTITION BY course_id) AS number_of_completions
FROM 
	viewflow_raw.courses c 
LEFT JOIN viewflow_raw.user_course uc ON uc.course_id = c.id	
WHERE uc.completed_at IS NOT NULL