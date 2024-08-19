import yaml

# header for source config yml file
yml_for_config = """version: 2

sources:
"""

# default path for dbt's dbt_project.yml file. needed to read leaner_query variables for use in source yml.
# assumes a standard dbt installation, i.e. packages are stored in './dbt_packages/package_name/'.
# Please modify if your installation is not standard
dbt_project_yml_filepath = "../../dbt_project.yml"

# path for leaner_query's data sources
source_yml_filepath = "models/staging/bigquery_audit_log/src_bigquery_audit_log.yml"

stg_model_query_text_filepath = (
    "models/staging/bigquery_audit_log/stg_bigquery_audit_log__data_access_query_text.txt"
)

stg_model_filepath = (
    "models/staging/bigquery_audit_log/stg_bigquery_audit_log__data_access.sql"
)


# load dict of variables from your dbt_project.yml file
def load_dbt_yml_vars(filename: str) -> dict:
    with open(f"{filename}") as f:
        dbt_yml = yaml.safe_load(f)
    dbt_yml_vars = dbt_yml["vars"]
    return dbt_yml_vars


# extract variable values from dict, presumably loaded in load_dbt_yml_vars.
# default values are provided, in case no variables are set in dbt_project.yml.
def extract_var_values(
    dbt_project_yml_vars: dict,
    var_names=[
        "leaner_query_database",
        "leaner_query_source_schema",
        "leaner_query_data_access_table",
    ],
    default_var_values=[
        "'{{ target.database }}'",
        "bigquery_audit_logs",
        "cloudaudit_googleapis_com_data_access",
    ],
) -> list:
    default_vars = zip(var_names, default_var_values)
    final_var_list = []
    for var_name, var_value in default_vars:
        var_list = []

        if var_name == "leaner_query_database":
            if var_name in dbt_project_yml_vars:
                bq_projects = dbt_project_yml_vars[f"{var_name}"]
            else:
                bq_projects = [f"{var_value}"]
            bq_project_list_length = len(bq_projects)
            final_var_list.append(bq_projects)

        elif var_name in dbt_project_yml_vars:
            if bq_project_list_length == len(dbt_project_yml_vars[f"{var_name}"]):
                var_list = dbt_project_yml_vars[f"{var_name}"]
            elif len(dbt_project_yml_vars[f"{var_name}"]) > bq_project_list_length:
                raise RuntimeError(
                    f"Too many entries in {var_name} list: please ensure the number of items matches with 'leaner_query_database'"
                )
            else:
                bq_project_list_length_modified = 0
                bq_project_list_length_modified = bq_project_list_length - len(
                    dbt_project_yml_vars[f"{var_name}"]
                )
                var_list = dbt_project_yml_vars[f"{var_name}"]
                i = 0
                while i < bq_project_list_length_modified:
                    var_list.append(f"{var_value}")
                    i += 1
            final_var_list.append(var_list)
        else:
            i = 0
            while i < bq_project_list_length:
                var_list.append(f"{var_value}")
                i += 1
            final_var_list.append(var_list)
    return final_var_list


# writes source yml file. loops through and gets corresponding values of each list.
def write_modified_yml_file(
    source_filename: str, bq_config_list: list, yml_headers: str
):
    yml_for_config_file = yml_headers
    i = 0
    while i < len(bq_config_list[0]):
        config_group = [j[i] for j in bq_config_list]
        if config_group[0] == "'{{ target.database }}'":
            data = f"""
  - name: bq_audit_default_project
    database: {config_group[0]}
    schema: {config_group[1]}
    tables:
      - name: {config_group[2]}
        description: BQ audit logs from default project.
"""
            yml_for_config_file += data
            i += 1

        else:
            data = f"""
  - name: bq_audit_{config_group[0].replace('-', '_')}
    database: {config_group[0]}
    schema: {config_group[1]}
    tables:
      - name: {config_group[2]}
        description: BQ audit logs from {config_group[0]} project.
"""
            yml_for_config_file += data
            i += 1

    with open(source_filename, "w", encoding="utf8") as out:
        out.write(yml_for_config_file)
    out.close()


# read staging txt file (does not contain "unioned" CTE)
def read_txt_file(filename: str) -> str:
    with open(filename, "r") as f:
        sql_string = f.read()
    return sql_string


# change macro to be local macro!
def generate_header(bq_config_list):
    stg_model_sql_header_part_one = """
with unioned as (
    {{ dbt_utils.union_relations( 
        relations=[
"""
    stg_model_sql_header_part_two = """
        ]
    ) }}
),\n
"""
    i = 0
    while i < len(bq_config_list[0]):
        config_group = [j[i] for j in bq_config_list]
        if config_group[0] == "'{{ target.database }}'":
            source = f"\t\tsource('bq_audit_default_project', '{config_group[2]}'),\n"
            stg_model_sql_header_part_one += source
            i += 1
        else:
            source = f"\t\tsource('bq_audit_{config_group[0].replace('-', '_')}', '{config_group[2]}'),\n"
            stg_model_sql_header_part_one += source
            i += 1
    header = stg_model_sql_header_part_one + stg_model_sql_header_part_two
    return header

# overwrites the staging file with updated sources.
def generate_sql_file_with_header(header_string: str, body_string: str, filename: str):
    with open(filename, "w+", encoding="utf8") as f:
        f.write(header_string)
        f.write(body_string)
    f.close()


if __name__ == "__main__":
    try:
        yml_results = load_dbt_yml_vars(dbt_project_yml_filepath)
        yml_config_list = extract_var_values(yml_results)
        write_modified_yml_file(source_yml_filepath, yml_config_list, yml_for_config)
        print("src_bigquery_audit_log.yml successfully overwritten!")
        stg_model = read_txt_file(stg_model_query_text_filepath)
        header = generate_header(yml_config_list)
        generate_sql_file_with_header(header, stg_model, stg_model_filepath)
        print("stg_bigquery_audit_log__data_access.sql successfully overwritten!")
    except Exception as e:
        print(f"Something went wrong. Error: {e}")
