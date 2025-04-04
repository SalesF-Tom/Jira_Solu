from google.cloud import bigquery

def Merge_Data_Projects_BQ(client, tabla_final, tabla_temp):
    merge_query = f"""
        MERGE `{tabla_final}` A
        USING (SELECT DISTINCT * FROM `{tabla_temp}`) B
        ON A.board_id = B.board_id
        
        WHEN MATCHED THEN
        UPDATE SET
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
    query_job = client.query(merge_query)
    resultados = list(query_job.result())
    filas_actualizadas = query_job.num_dml_affected_rows
    print(f"\033[35m Se actualizaron {filas_actualizadas} filas en la tabla {tabla_final}. \033[0m")

def Merge_Data_Sprints_BQ(client, tabla_final, tabla_temp):
    """
    Realiza un MERGE en BigQuery para actualizar e insertar datos en la tabla final desde la tabla temporal.
    """
    merge_query = f"""
        MERGE `{tabla_final}` A
        USING (SELECT DISTINCT * FROM `{tabla_temp}`) B
        ON A.sprint_id = B.sprint_id AND A.board_id = B.board_id
        
        WHEN MATCHED THEN
        UPDATE SET
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
    try:
        query_job = client.query(merge_query)
        query_job.result()  # Esperar a que termine la ejecución del query
        filas_actualizadas = query_job.num_dml_affected_rows
        print(f"\033[35m Se actualizaron {filas_actualizadas} filas en la tabla {tabla_final}. \033[0m")
    except Exception as e:
        print(f"\033[31m Error al realizar el MERGE en la tabla {tabla_final}: {e} \033[0m")
        raise  # Detener el flujo si ocurre un error

def Merge_Data_Tickets_BQ(client, tabla_final, tabla_temp):
    merge_query = f"""
        MERGE `{tabla_final}` A
        USING (SELECT DISTINCT * FROM `{tabla_temp}`) B
        ON A.ticket_id = B.ticket_id AND A.sprint_id = B.sprint_id
        
        WHEN MATCHED THEN
        UPDATE SET
            A.sprint_id = B.sprint_id,
            A.ticket_key = B.ticket_key,
            A.ticket_summary = B.ticket_summary,
            A.ticket_status = B.ticket_status,
            A.ticket_assignee = B.ticket_assignee,
            A.ticket_priority = B.ticket_priority,
            A.ticket_type = B.ticket_type,
            A.ticket_created = B.ticket_created,
            A.ticket_original_estimate=B.ticket_original_estimate,
            A.ticket_updated = B.ticket_updated,
            A.ticket_resolution = B.ticket_resolution,
            A.ticket_labels = B.ticket_labels

        WHEN NOT MATCHED THEN
        INSERT (
            ticket_id, sprint_id, ticket_key, ticket_summary, ticket_status, ticket_assignee,
            ticket_priority, ticket_type, ticket_created, ticket_original_estimate ,ticket_updated, ticket_resolution, ticket_labels
        )
        VALUES (
            B.ticket_id, B.sprint_id, B.ticket_key, B.ticket_summary, B.ticket_status, B.ticket_assignee,
            B.ticket_priority, B.ticket_type, B.ticket_created, B.ticket_original_estimate,B.ticket_updated, B.ticket_resolution, B.ticket_labels
        )
    """
    query_job = client.query(merge_query)
    resultados = list(query_job.result())
    filas_actualizadas = query_job.num_dml_affected_rows
    print(f"\033[35m Se actualizaron {filas_actualizadas} filas en la tabla {tabla_final}. \033[0m")

