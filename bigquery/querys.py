from google.cloud import bigquery

def Merge_Data_Projects_BQ(client, tabla_final, tabla_temp):
    merge_query = f"""
        MERGE `{tabla_final}` A
        USING (SELECT DISTINCT * FROM `{tabla_temp}`) B
        ON A.board_id = B.board_id
        
        WHEN MATCHED
        #   AND (
        #     A.board_name IS DISTINCT FROM B.board_name OR
        #     A.board_type IS DISTINCT FROM B.board_type OR
        #     A.project_key IS DISTINCT FROM B.project_key OR
        #     A.project_name IS DISTINCT FROM B.project_name OR
        #     A.project_location IS DISTINCT FROM B.project_location
        # )
        THEN UPDATE SET
            A.board_name = B.board_name,
            A.board_type = B.board_type,
            A.project_key = B.project_key,
            A.project_name = B.project_name,
            A.project_location = B.project_location

        WHEN NOT MATCHED THEN
        INSERT (
            board_id, board_name, board_type, project_key, project_name, project_location
        )
        VALUES (
            B.board_id, B.board_name, B.board_type, B.project_key, B.project_name, B.project_location
        )
    """
    # print(merge_query)

    query_job = client.query(merge_query)
    query_job.result()
    print(f"\033[35m Se actualizaron {query_job.num_dml_affected_rows} filas en la tabla {tabla_final}. \033[0m")
    bytes_facturados = query_job.estimated_bytes_processed
    megabytes_facturados = bytes_facturados / (1024 * 1024)
    print(f"Bytes facturados: {megabytes_facturados:.2f} MB")
    return megabytes_facturados


def Merge_Data_Sprints_BQ(client, tabla_final, tabla_temp):
    merge_query = f"""
        MERGE `{tabla_final}` A
        USING (SELECT DISTINCT * FROM `{tabla_temp}`) B
        ON A.sprint_id = B.sprint_id AND A.board_id = B.board_id

        WHEN MATCHED
            # AND (
            #     A.sprint_name IS DISTINCT FROM B.sprint_name OR
            #     A.sprint_state IS DISTINCT FROM B.sprint_state OR
            #     A.sprint_startDate IS DISTINCT FROM B.sprint_startDate OR
            #     A.sprint_endDate IS DISTINCT FROM B.sprint_endDate OR
            #     A.sprint_completeDate IS DISTINCT FROM B.sprint_completeDate OR
            #     A.sprint_goal IS DISTINCT FROM B.sprint_goal
            # )
        THEN UPDATE SET
            A.sprint_name = B.sprint_name,
            A.sprint_state = B.sprint_state,
            A.sprint_startDate = B.sprint_startDate,
            A.sprint_endDate = B.sprint_endDate,
            A.sprint_completeDate = B.sprint_completeDate,
            A.sprint_goal = B.sprint_goal

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

    # print(merge_query)

    query_job = client.query(merge_query)
    query_job.result()
    print(f"\033[35m Se actualizaron {query_job.num_dml_affected_rows} filas en la tabla {tabla_final}. \033[0m")
    bytes_facturados = query_job.estimated_bytes_processed
    megabytes_facturados = bytes_facturados / (1024 * 1024)
    print(f"Bytes facturados: {megabytes_facturados:.2f} MB")
    return megabytes_facturados


def Merge_Data_Tickets_BQ(client, tabla_final, tabla_temp):
    merge_query = f"""
        MERGE `{tabla_final}` A
        USING (SELECT DISTINCT * FROM `{tabla_temp}`) B
        ON A.ticket_id = B.ticket_id AND A.sprint_id = B.sprint_id

        WHEN MATCHED
            # AND (
            #     A.ticket_key IS DISTINCT FROM B.ticket_key OR
            #     A.ticket_summary IS DISTINCT FROM B.ticket_summary OR
            #     A.ticket_status IS DISTINCT FROM B.ticket_status OR
            #     A.ticket_assignee IS DISTINCT FROM B.ticket_assignee OR
            #     A.ticket_priority IS DISTINCT FROM B.ticket_priority OR
            #     A.ticket_type IS DISTINCT FROM B.ticket_type OR
            #     A.ticket_created IS DISTINCT FROM B.ticket_created OR
            #     A.ticket_original_estimate IS DISTINCT FROM B.ticket_original_estimate OR
            #     A.ticket_updated IS DISTINCT FROM B.ticket_updated OR
            #     A.ticket_resolution IS DISTINCT FROM B.ticket_resolution OR
            #     A.ticket_labels IS DISTINCT FROM B.ticket_labels
            # )
        THEN UPDATE SET
            A.ticket_key = B.ticket_key,
            A.ticket_summary = B.ticket_summary,
            A.ticket_status = B.ticket_status,
            A.ticket_assignee = B.ticket_assignee,
            A.ticket_priority = B.ticket_priority,
            A.ticket_type = B.ticket_type,
            A.ticket_created = B.ticket_created,
            A.ticket_original_estimate = B.ticket_original_estimate,
            A.ticket_updated = B.ticket_updated,
            A.ticket_resolution = B.ticket_resolution,
            A.ticket_labels = B.ticket_labels

        WHEN NOT MATCHED THEN
        INSERT (
            ticket_id, sprint_id, ticket_key, ticket_summary, ticket_status, ticket_assignee,
            ticket_priority, ticket_type, ticket_created, ticket_original_estimate, ticket_updated, ticket_resolution, ticket_labels
        )
        VALUES (
            B.ticket_id, B.sprint_id, B.ticket_key, B.ticket_summary, B.ticket_status, B.ticket_assignee,
            B.ticket_priority, B.ticket_type, B.ticket_created, B.ticket_original_estimate, B.ticket_updated, B.ticket_resolution, B.ticket_labels
        )
    """

    # print(merge_query)

    query_job = client.query(merge_query)
    query_job.result()
    print(f"\033[35m Se actualizaron {query_job.num_dml_affected_rows} filas en la tabla {tabla_final}. \033[0m")
    bytes_facturados = query_job.estimated_bytes_processed
    megabytes_facturados = bytes_facturados / (1024 * 1024)
    print(f"Bytes facturados: {megabytes_facturados:.2f} MB")
    return megabytes_facturados


