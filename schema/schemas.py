from google.cloud import bigquery

class Esquema(object):
    schema_projects = [
    bigquery.SchemaField("board_id", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("board_name", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("board_type", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("project_key", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("project_name", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("project_location", "STRING", mode="NULLABLE"),
]
    schema_sprints = [
    bigquery.SchemaField("board_id", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("sprint_id", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("sprint_name", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("sprint_state", "STRING", mode="NULLABLE"),  # future, active, closed
    bigquery.SchemaField("sprint_startDate", "DATETIME", mode="NULLABLE"),
    bigquery.SchemaField("sprint_endDate", "DATETIME", mode="NULLABLE"),
    bigquery.SchemaField("sprint_completeDate", "DATETIME", mode="NULLABLE"),
    bigquery.SchemaField("sprint_goal", "STRING", mode="NULLABLE"),
]
    schema_tickets = [
    bigquery.SchemaField("sprint_id", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("ticket_id", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("ticket_key", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("ticket_summary", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("ticket_status", "STRING", mode="NULLABLE"),  # Ej: To Do, In Progress, Done
    bigquery.SchemaField("ticket_assignee", "STRING", mode="NULLABLE"),  # Nombre de la persona asignada
    bigquery.SchemaField("ticket_priority", "STRING", mode="NULLABLE"),  # Ej: High, Medium, Low
    bigquery.SchemaField("ticket_type", "STRING", mode="NULLABLE"),  # Ej: Bug, Task, Story
    bigquery.SchemaField("ticket_created", "DATETIME", mode="NULLABLE"),
    bigquery.SchemaField("ticket_original_estimate", "INTEGER", mode="NULLABLE"),
    bigquery.SchemaField("ticket_updated", "DATETIME", mode="NULLABLE"),
    bigquery.SchemaField("ticket_resolution", "STRING", mode="NULLABLE"),  # Ej: Fixed, Won't Fix
    bigquery.SchemaField("ticket_labels", "STRING", mode="NULLABLE"),  # Comma-separated labels
]
