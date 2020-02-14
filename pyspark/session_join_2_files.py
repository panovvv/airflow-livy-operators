""" todo """

from pyspark.shell import spark

file1_df_params_unfiltered = {
    "path": "{{ params.file1_path }}",
    "sep": "{{ params.file1_sep }}",
    "inferSchema": "{{ params.file1_infer_schema }}",
    "schema": "{{ params.file1_schema }}",
    "header": "{{ params.file1_header }}",
    "quote": "{{ params.file1_quote }}",
    "escape": "{{ params.file1_escape }}",
}
file1_df_params = {k: v for k, v in file1_df_params_unfiltered.items() if v}
file1_df = spark.read.csv(**file1_df_params)
file1_df.createOrReplaceTempView("file1")

file2_df_params_unfiltered = {
    "path": "{{ params.file2_path }}",
    "sep": "{{ params.file2_sep }}",
    "inferSchema": "{{ params.file2_infer_schema }}",
    "schema": "{{ params.file2_schema }}",
    "header": "{{ params.file2_header }}",
    "quote": "{{ params.file2_quote }}",
    "escape": "{{ params.file2_escape }}",
}
file2_df_params = {k: v for k, v in file2_df_params_unfiltered.items() if v}
file2_df = spark.read.csv(**file2_df_params)
file2_df.createOrReplaceTempView("file2")

sql = """
SELECT
    {{ params.output_columns }}
FROM file1
JOIN file2
ON file1.{{ params.file1_join_column }} = file2.{{ params.file2_join_column }}
"""
output = spark.sql(sql)

output.show()

# Uncomment lines below to save the result to a file.

# Run ID contains a date with semicolons (:), most URIs can't contain those.
# safe_run_id = '{{ run_id|replace(":", "-") }}'
# output_params = {
#     "sep": "{{ params.output_sep }}",
#     "header": "{{ params.output_header }}",
#     "mode": "{{ params.output_mode }}",
#     "path": f"{{ params.output_path_base }}/{safe_run_id}/{{ task.task_id }}",
# }
# output.write.csv(**output_params)
