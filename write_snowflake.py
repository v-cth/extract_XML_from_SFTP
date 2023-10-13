import pandas as pd
import snowflake.connector

# Configure Snowflake connection
conn = snowflake.connector.connect(
    user='test',
    password='pwd',
    account='host',
    warehouse='default',
    database='prod',
    schema='analytics'
)

# Write DataFrame in a Snowflake table fct_results (must have the same fields as dataframe)
result_table.to_sql('fct_results', conn, if_exists='append', index=False) 

# Close Snowflake connection
conn.close()
