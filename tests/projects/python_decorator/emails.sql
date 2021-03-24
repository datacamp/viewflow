/* 
---
owner: engineering@datacamp.com
description: Get all unique email addresses
fields:
  email: The unique email addresses in the users table
schema: viewflow
connection_id: postgres_viewflow
--- 
*/

SELECT DISTINCT email FROM viewflow.users
