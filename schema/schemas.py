from google.cloud import bigquery

class Esquema(object):
    schema_projects = [
        bigquery.SchemaField("board_id",         "STRING"),
        bigquery.SchemaField("board_name",       "STRING"),
        bigquery.SchemaField("board_type",       "STRING"),
        bigquery.SchemaField("project_key",      "STRING"),
        bigquery.SchemaField("project_name",     "STRING"),
        bigquery.SchemaField("project_location", "STRING"),
    ]

    schema_sprints = [
        bigquery.SchemaField("board_id",            "STRING"),
        bigquery.SchemaField("sprint_id",           "STRING"),
        bigquery.SchemaField("sprint_name",         "STRING"),
        bigquery.SchemaField("sprint_state",        "STRING"),   # future, active, closed
        bigquery.SchemaField("sprint_startDate",    "DATETIME"),
        bigquery.SchemaField("sprint_endDate",      "DATETIME"),
        bigquery.SchemaField("sprint_completeDate", "DATETIME"),
        bigquery.SchemaField("sprint_goal",         "STRING"),
    ]

    schema_tickets = [
        bigquery.SchemaField("sprint_id",                  "STRING"),
        bigquery.SchemaField("ticket_id",                  "STRING"),
        bigquery.SchemaField("ticket_key",                 "STRING"),
        bigquery.SchemaField("ticket_summary",             "STRING"),
        bigquery.SchemaField("ticket_status",              "STRING"),  # To Do, In Progress, Done, etc.
        bigquery.SchemaField("ticket_assignee",            "STRING"),
        bigquery.SchemaField("ticket_priority",            "STRING"),
        bigquery.SchemaField("ticket_type",                "STRING"),  # Bug, Task, Story, etc.
        bigquery.SchemaField("ticket_created",             "DATETIME"),
        bigquery.SchemaField("ticket_original_estimate",   "FLOAT"),
        bigquery.SchemaField("ticket_updated",             "DATETIME"),
        bigquery.SchemaField("ticket_resolution",          "STRING"),
        bigquery.SchemaField("ticket_labels",              "STRING"),
        # 👇 nuevos campos para épica
        bigquery.SchemaField("epic_key",                   "STRING"),
        bigquery.SchemaField("epic_name",                  "STRING"),
    ]

    schema_users = [
        bigquery.SchemaField("account_id",          "STRING"),
        bigquery.SchemaField("account_type",        "STRING"),
        bigquery.SchemaField("account_status",      "STRING"),
        bigquery.SchemaField("display_name",        "STRING"),
        bigquery.SchemaField("public_name",         "STRING"),
        bigquery.SchemaField("email_address",       "STRING"),
        bigquery.SchemaField("active",              "BOOL"),
        bigquery.SchemaField("time_zone",           "STRING"),
        bigquery.SchemaField("locale",              "STRING"),
        bigquery.SchemaField("self_url",            "STRING"),
        bigquery.SchemaField("avatar_16x16",        "STRING"),
        bigquery.SchemaField("avatar_24x24",        "STRING"),
        bigquery.SchemaField("avatar_32x32",        "STRING"),
        bigquery.SchemaField("avatar_48x48",        "STRING"),
        bigquery.SchemaField("groups",              "STRING"),
        bigquery.SchemaField("application_roles",   "STRING"),
    ]
