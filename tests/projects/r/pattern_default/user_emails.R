# ---
# owner: data@datacamp.com
# description: This is a very simple R task.
# fields:
#   user_id: User ID
#   email: Email address
# schema: viewflow
# connection_id: postgres_viewflow
# ---

user_emails = select(viewflow.users, c("user_id", "email"))
