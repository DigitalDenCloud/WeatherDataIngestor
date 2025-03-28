# Script generated for node MySQL
MySQL_node = glueContext.write_dynamic_frame.from_jdbc_conf(
frame=<SOURCE_FRAME_VARIABLE>,  # replace <SOURCE_FRAME_VARIABLE> with your transformed data frame variable (for example: ChangeSchema_node1743169166145)
catalog_connection="weather-aurora-conn",
connection_options={"database": "weatherdb", "dbtable": "weather_data"},
transformation_ctx="MySQL_node"
)