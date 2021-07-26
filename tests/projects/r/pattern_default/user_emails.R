# ---
# owner: data@datacamp.com
# description: This is a very simple R task.
# fields:
#   user_id: User ID
#   email: Email address
# schema: viewflow
# connection_id: postgres_viewflow
# on_success_callback: failure_callback_email
# on_retry_callback: failure_callback_email
# on_failure_callback: failure_callback_email
# ---

user_emails = select(viewflow.users, c("user_id", "email"))
