"""
This code just joins two files on any column.
The result can be saved to a file.
"""

import argparse
import json

from pyspark.shell import spark

SAME_DOC = "Same deal as for file 1"

parser = argparse.ArgumentParser(formatter_class=argparse.ArgumentDefaultsHelpFormatter)
parser.add_argument(
    "file1_path",
    type=str,
    help="Input file location. Available schemas are: file, hdfs, s3",
)
parser.add_argument(
    "-file1_sep",
    type=str,
    default="\t",
    help="Character that separates records in each line of file",
)
parser.add_argument(
    "-file1_quote",
    type=str,
    default='"',
    help="Use quote character if fields in your file "
    "contain separator characters in them.",
)
parser.add_argument(
    "-file1_escape",
    type=str,
    default="\\",
    help="Use escape character together with quote character to be able to include "
    'quote characters as part of data fields (e.g. \\" ).',
)
parser.add_argument(
    "-file1_header",
    type=str,
    required=True,
    help="Whether the file contains a header line or not ('true' or 'false').",
)
parser.add_argument(
    "-file1_schema",
    type=str,
    required=True,
    help="Can be specified as DDL-like string e.g. col1 BIGINT, col2 STRING. "
    "Spark can actually infer schema from the file, but it scans the whole file "
    "in order to do that - therefore it takes ages with big files.",
)
parser.add_argument(
    "-file1_join_column",
    type=str,
    required=True,
    help="Name of the column inside the file to use when joininig with the other file",
)

parser.add_argument("file2_path", type=str, help=SAME_DOC)
parser.add_argument("-file2_sep", type=str, default="\t", help=SAME_DOC)
parser.add_argument("-file2_quote", type=str, default='"', help=SAME_DOC)
parser.add_argument("-file2_escape", type=str, default="\\", help=SAME_DOC)
parser.add_argument("-file2_header", type=str, required=True, help=SAME_DOC)
parser.add_argument("-file2_schema", type=str, required=True, help=SAME_DOC)
parser.add_argument("-file2_join_column", type=str, required=True, help=SAME_DOC)

parser.add_argument(
    "-output_path",
    type=str,
    help="Location to store result at. Available schemas are: file, hdfs, s3",
)
parser.add_argument("-output_sep", type=str, default="\t", help=SAME_DOC)
parser.add_argument(
    "-output_header", type=str, help="Note that the header is not required this time."
)
parser.add_argument(
    "-output_mode",
    type=str,
    default="overwrite",
    help="Write mode for output file. "
    "https://spark.apache.org/docs/latest/sql-data-sources-load-save-functions.html"
    "#save-modes",
)
parser.add_argument(
    "-output_columns",
    type=str,
    help="List of columns you'd specify in the SELECT statement. "
    "Available aliases are file1 and file2. "
    "E.g. 'file1.`First Name`, file2.Address1'",
)

args = parser.parse_args()

file1_df_params = {
    "path": args.file1_path,
    "sep": args.file1_sep,
    "quote": args.file1_quote,
    "escape": args.file1_escape,
    "inferSchema": "false",
    "header": args.file1_header,
    "schema": args.file1_schema,
}
print(f"File 1 dataframe parameters:\n{json.dumps(file1_df_params, indent=2)}")
file1_df = spark.read.csv(**file1_df_params)
file1_df.createOrReplaceTempView("file1")

file2_df_params = {
    "path": args.file2_path,
    "sep": args.file2_sep,
    "quote": args.file2_quote,
    "escape": args.file2_escape,
    "inferSchema": "false",
    "header": args.file2_header,
    "schema": args.file2_schema,
}
print(f"File 2 dataframe parameters:\n{json.dumps(file2_df_params, indent=2)}")
file2_df = spark.read.csv(**file2_df_params)
file2_df.createOrReplaceTempView("file2")

sql = f"""
SELECT
    {args.output_columns}
FROM file1
JOIN file2 ON file1.{args.file1_join_column} = file2.{args.file2_join_column}
"""
print(f"SQL query:\n{sql}")
output = spark.sql(sql)

output.show(50)

if args.output_path:
    output_params = {
        "sep": args.output_sep,
        "header": args.output_header,
        "mode": args.output_mode,
        "path": args.output_path,
    }
    output.write.csv(**output_params)
