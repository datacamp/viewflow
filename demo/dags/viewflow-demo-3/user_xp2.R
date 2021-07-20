# ---
# owner: viewflow-team
# description: Provide the total amount of XP for each user
# fields:
#   user_id: The user id
#   xp: The sum of XP
# schema: viewflow_demo
# connection_id: postgres_demo
# ---

library(dplyr)

temp = select(merge(viewflow_raw.users, viewflow_raw.user_course, by.x='id', by.y='user_id', all.x=TRUE), c('id', 'course_id'))
names(temp)[names(temp) == 'id'] <- 'user_id'
user_all_xp = select(merge(temp, viewflow_raw.courses, by.x='course_id', by.y='id'), c('user_id', 'xp'))

user_xp2 = aggregate(xp ~ user_id, user_all_xp, sum)
