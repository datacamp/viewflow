/* 
---
owner: engineering@datacamp.com
description: Description
fields:
  course_id: The id of the course
  title: The title of the course
  technology: The technology of the course
schema: viewflow
--- 
*/

SELECT course_id, title, technology, exercise_title
  FROM viewflow.courses c
  JOIN viewflow.exercises e using (course_id) 
