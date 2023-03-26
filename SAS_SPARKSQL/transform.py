Step 2: Transform SAS Commands into SQL Queries

#a dictionary of SAS-to-Snowflake command mappings
sas_to_sql = {
    "PROC SQL": "SELECT",
    "DATA": "CREATE TABLE",
    "RUN": ";",
    "QUIT": ""
}

#a UDF to transform SAS commands into SQL queries
def transform_sas_command(sas_command):
    for sas, sql in sas_to_sql.items():
        if sas in sas_command:
            return sas_command.replace(sas, sql)
    return ""

# Apply the UDF to the relevant lines of the SAS script
sql_queries_df = relevant_lines_df.selectExpr(
    "transform_sas_command(value) as sql_query"
)

# Filter out any empty SQL queries
non_empty_sql_queries_df = sql_queries_df.filter(
    lambda row: row.sql_query != ""
)

# Show the resulting SQL queries for debugging purposes
non_empty_sql_queries_df.show()
