from google.cloud import bigquery

def Merge_Data_Projects_BQ(client: bigquery.Client, tabla_final: str, tabla_temp: str):
    merge_query = f"""
        MERGE `{tabla_final}` A
        USING (SELECT DISTINCT * FROM `{tabla_temp}`) B
        ON A.board_id = B.board_id
        
        WHEN MATCHED THEN
        UPDATE SET
            A.board_name       = B.board_name,
            A.board_type       = B.board_type,
            A.project_key      = B.project_key,
            A.project_name     = B.project_name,
            A.project_location = B.project_location

        WHEN NOT MATCHED THEN
        INSERT (board_id, board_name, board_type, project_key, project_name, project_location)
        VALUES (B.board_id, B.board_name, B.board_type, B.project_key, B.project_name, B.project_location)
    """
    job = client.query(merge_query)
    job.result()
    print(f"\033[35m Se actualizaron {job.num_dml_affected_rows} filas en {tabla_final}. \033[0m")
    mb = (job.estimated_bytes_processed or 0) / (1024 * 1024)
    print(f"Bytes facturados: {mb:.2f} MB")
    return mb


def Merge_Data_Sprints_BQ(client: bigquery.Client, tabla_final: str, tabla_temp: str):
    merge_query = f"""
        MERGE `{tabla_final}` A
        USING (SELECT DISTINCT * FROM `{tabla_temp}`) B
        ON A.sprint_id = B.sprint_id AND A.board_id = B.board_id

        WHEN MATCHED THEN
        UPDATE SET
            A.sprint_name        = B.sprint_name,
            A.sprint_state       = B.sprint_state,
            A.sprint_startDate   = B.sprint_startDate,
            A.sprint_endDate     = B.sprint_endDate,
            A.sprint_completeDate= B.sprint_completeDate,
            A.sprint_goal        = B.sprint_goal

        WHEN NOT MATCHED THEN
        INSERT (
            sprint_id, board_id, sprint_name, sprint_state, 
            sprint_startDate, sprint_endDate, sprint_completeDate, sprint_goal
        )
        VALUES (
            B.sprint_id, B.board_id, B.sprint_name, B.sprint_state, 
            B.sprint_startDate, B.sprint_endDate, B.sprint_completeDate, B.sprint_goal
        )
    """
    job = client.query(merge_query)
    job.result()
    print(f"\033[35m Se actualizaron {job.num_dml_affected_rows} filas en {tabla_final}. \033[0m")
    mb = (job.estimated_bytes_processed or 0) / (1024 * 1024)
    print(f"Bytes facturados: {mb:.2f} MB")
    return mb


def Merge_Data_Tickets_BQ(client: bigquery.Client, tabla_final: str, tabla_temp: str):
    """
    IMPORTANTE: Esta versión incluye epic_key y epic_name.
    Asegurate de haber creado/ajustado el esquema de tabla_final y de tabla_temp con esos campos.
    """
    merge_query = f"""
        MERGE `{tabla_final}` A
        USING (SELECT DISTINCT * FROM `{tabla_temp}`) B
        ON A.ticket_id = B.ticket_id
       AND A.sprint_id IS NOT DISTINCT FROM B.sprint_id

        WHEN MATCHED THEN
        UPDATE SET
            A.ticket_key                = B.ticket_key,
            A.ticket_summary            = B.ticket_summary,
            A.ticket_status             = B.ticket_status,
            A.ticket_assignee           = B.ticket_assignee,
            A.ticket_priority           = B.ticket_priority,
            A.ticket_type               = B.ticket_type,
            A.ticket_created            = B.ticket_created,
            A.ticket_original_estimate  = B.ticket_original_estimate,
            A.ticket_updated            = B.ticket_updated,
            A.ticket_resolution         = B.ticket_resolution,
            A.ticket_labels             = B.ticket_labels,
            A.epic_key                  = B.epic_key,
            A.epic_name                 = B.epic_name

        WHEN NOT MATCHED THEN
        INSERT (
            ticket_id, sprint_id, ticket_key, ticket_summary, ticket_status, ticket_assignee,
            ticket_priority, ticket_type, ticket_created, ticket_original_estimate,
            ticket_updated, ticket_resolution, ticket_labels, epic_key, epic_name
        )
        VALUES (
            B.ticket_id, B.sprint_id, B.ticket_key, B.ticket_summary, B.ticket_status, B.ticket_assignee,
            B.ticket_priority, B.ticket_type, B.ticket_created, B.ticket_original_estimate,
            B.ticket_updated, B.ticket_resolution, B.ticket_labels, B.epic_key, B.epic_name
        )
    """
    job = client.query(merge_query)
    job.result()
    print(f"\033[35m Se actualizaron {job.num_dml_affected_rows} filas en {tabla_final}. \033[0m")
    mb = (job.estimated_bytes_processed or 0) / (1024 * 1024)
    print(f"Bytes facturados: {mb:.2f} MB")
    return mb


def Merge_Data_Users_BQ(client: bigquery.Client, tabla_final: str, tabla_temp: str):
    merge_query = f"""
        MERGE `{tabla_final}` A
        USING (SELECT DISTINCT * FROM `{tabla_temp}`) B
        ON A.account_id = B.account_id

        WHEN MATCHED THEN
        UPDATE SET
            A.account_type       = B.account_type,
            A.account_status     = B.account_status,
            A.display_name       = B.display_name,
            A.public_name        = B.public_name,
            A.email_address      = B.email_address,
            A.active             = B.active,
            A.time_zone          = B.time_zone,
            A.locale             = B.locale,
            A.self_url           = B.self_url,
            A.avatar_16x16       = B.avatar_16x16,
            A.avatar_24x24       = B.avatar_24x24,
            A.avatar_32x32       = B.avatar_32x32,
            A.avatar_48x48       = B.avatar_48x48,
            A.`groups`           = B.`groups`,
            A.application_roles  = B.application_roles

        WHEN NOT MATCHED THEN
        INSERT (
            account_id,
            account_type,
            account_status,
            display_name,
            public_name,
            email_address,
            active,
            time_zone,
            locale,
            self_url,
            avatar_16x16,
            avatar_24x24,
            avatar_32x32,
            avatar_48x48,
            `groups`,
            application_roles
        )
        VALUES (
            B.account_id,
            B.account_type,
            B.account_status,
            B.display_name,
            B.public_name,
            B.email_address,
            B.active,
            B.time_zone,
            B.locale,
            B.self_url,
            B.avatar_16x16,
            B.avatar_24x24,
            B.avatar_32x32,
            B.avatar_48x48,
            B.`groups`,
            B.application_roles
        )
    """
    job = client.query(merge_query)
    job.result()
    print(f"\033[35m Se actualizaron {job.num_dml_affected_rows} filas en {tabla_final}. \033[0m")
    mb = (job.estimated_bytes_processed or 0) / (1024 * 1024)
    print(f"Bytes facturados: {mb:.2f} MB")
    return mb
