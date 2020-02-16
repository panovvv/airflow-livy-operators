""" todo """

from pyspark.shell import spark

file1_df_params_unfiltered = {
    "path": "{{ file1_path }}",
    "sep": "{{ file1_sep }}",
    "inferSchema": "{{ file1_infer_schema }}",
    "schema": "{{ file1_schema }}",
    "header": "{{ file1_header }}",
    "quote": "{{ file1_quote }}",
    "escape": "{{ file1_escape }}",
}
file1_df_params = {k: v for k, v in file1_df_params_unfiltered.items() if v}
file1_df = spark.read.csv(**file1_df_params)
file1_df.createOrReplaceTempView("file1")

file2_df_params_unfiltered = {
    "path": "{{ file2_path }}",
    "sep": "{{ file2_sep }}",
    "inferSchema": "{{ file2_infer_schema }}",
    "schema": "{{ file2_schema }}",
    "header": "{{ file2_header }}",
    "quote": "{{ file2_quote }}",
    "escape": "{{ file2_escape }}",
}
file2_df_params = {k: v for k, v in file2_df_params_unfiltered.items() if v}
file2_df = spark.read.csv(**file2_df_params)
file2_df.createOrReplaceTempView("file2")

sql = """
SELECT
    {{ output_columns }}
FROM file1
JOIN file2
ON file1.{{ file1_join_column }} = file2.{{ file2_join_column }}
"""
output = spark.sql(sql)

print("First 50 lines of result:")
output.show(50)

# Uncomment lines below to save the result to a file.
# Run ID contains a date with semicolons (:), most URIs can't contain those.
# safe_run_id = '{{ run_id|replace(":", "-") }}'
# output_params = {
#     "sep": "{{ output_sep }}",
#     "header": "{{ output_header }}",
#     "mode": "{{ output_mode }}",
#     "path": f"{{ output_path }}/{safe_run_id}/{{ task.task_id }}",
# }
# output.write.csv(**output_params)
