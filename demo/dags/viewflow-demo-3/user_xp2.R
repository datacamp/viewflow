# ---
# owner: viewflow-team
# description: Provide the total amount of XP for each user
# fields:
#   user_id: The user id
#   xp: The sum of XP
# schema: viewflow_demo
# connection_id: postgres_demo
# ---


# FETCH DATA
# TODO automate this process
# Libraries: DBI, RPostgres (dplyr for user's script)
library(DBI)


con <- dbConnect(RPostgres::Postgres(),
    dbname = 'airflow', 
    host = 'localhost',
    port = 5433,
    user = 'airflow',
    password = 'airflow',
)

viewflow_raw.users =       dbReadTable(con, name = Id(schema = 'viewflow_raw', table = 'users'))
viewflow_raw.user_course = dbReadTable(con, name = Id(schema = 'viewflow_raw', table = 'user_course'))
viewflow_raw.courses =     dbReadTable(con, name = Id(schema = 'viewflow_raw', table = 'courses'))

dbDisconnect(con)


# ACTUAL CREATION OF VIEW
library(dplyr)
temp = select(merge(viewflow_raw.users, viewflow_raw.user_course, by.x='id', by.y='user_id', all.x=TRUE), c('id', 'course_id'))
names(temp)[names(temp) == 'id'] <- 'user_id'
user_all_xp = select(merge(temp, viewflow_raw.courses, by.x='course_id', by.y='id'), c('user_id', 'xp'))
user_xp2 = aggregate(xp ~ user_id, user_all_xp, sum)


# MATERIALIZE THE VIEW
dbWriteTable(con, name = Id(schema = 'viewflow_demo', table = 'user_xp2'), user_xp2, overwrite=TRUE)