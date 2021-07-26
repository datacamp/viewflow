# ---
# owner: data@datacamp.com
# description: This is a very simple R task.
# fields:
#   user_id: User ID
#   email: Email address
# schema: viewflow
# connection_id: postgres_viewflow
# dependency_function: custom_get_dependencies
# ---

user_emails = select(myPrefix.viewflow.users, c("user_id", "email"))
