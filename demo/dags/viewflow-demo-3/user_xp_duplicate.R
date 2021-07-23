# ---
# owner: viewflow-team
# description: Provide the total amount of XP for each user
# fields:
#   user_id: The user id
#   xp: The sum of XP
# schema: viewflow_demo
# connection_id: postgres_demo
# dependency_function: custom_get_dependencies
# ---

library(dplyr)

# The custom_get_dependencies function defines that tables are referenced as myPrefix.<schema_name>.<table_name>
users <- myPrefix.viewflow_raw.users
user_course <- myPrefix.viewflow_raw.user_course
temp <- select(merge(users, user_course, by.x='id', by.y='user_id', all.x=TRUE), c('id', 'course_id'))
names(temp)[names(temp) == 'id'] <- 'user_id'

user_all_xp <- select(merge(temp, myPrefix.viewflow_raw.courses, by.x='course_id', by.y='id'), c('user_id', 'xp'))

user_xp_duplicate <- aggregate(xp ~ user_id, user_all_xp, sum)[order(user_id)]
