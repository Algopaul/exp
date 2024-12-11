"""Collection of hacky and unsafe database operations."""
import os
from hashlib import md5
import shutil


def hash_string(given):
  s = str(given)
  return md5(s.encode()).hexdigest()


def dict_to_constraints(d):
  return ' AND '.join([f'{k}={v}' for k, v in d.items()])


def list_to_constraints(li):
  return ' AND '.join([l for l in li])


def print_table_to_file(table, filename):
  query = f".output '{filename}'\n"
  query += ".mode csv\n"
  query += ".header on\n"
  query += ".separator ', '\n"
  query += f"SELECT * FROM {table};\n"
  return query


def common_table(common):
  hash = 'a' + hash_string(common)  # needs letter to start
  query = ''
  query += f'DROP TABLE IF EXISTS {hash};\n'
  query += f'CREATE TABLE {hash} AS SELECT * FROM results WHERE {list_to_constraints(common)};\n'
  return query, hash


def casewhen_table(
    group_field,
    cases,
    hash,
    res_field,
    experiment_name,
    tablename=None,
):
  if tablename is None:
    tablename = f'data/results_tables/{experiment_name}/{res_field}'
  else:
    tablename = f'data/results_tables/{experiment_name}/{tablename}'
  query = ''
  query += f'CREATE TABLE {res_field} AS\n'
  query += 'SELECT\n'
  query += f'  {group_field},\n'
  for c in cases[:len(cases) - 1]:
    query += f"  MIN(CASE WHEN {list_to_constraints(c['constraints'])} THEN {res_field} END) AS {c['name']},\n"
  query += f"  MIN(CASE WHEN {list_to_constraints(cases[-1]['constraints'])} THEN {res_field} END) AS {cases[-1]['name']}\n"
  query += f'FROM {hash}\n'
  query += f'GROUP BY {group_field};\n'
  query += print_table_to_file(res_field, tablename)
  query += f'DROP TABLE {res_field};\n'
  return query


def secondary_table(
    group_field,
    secondary_field,
    cases,
    hash,
    res_field,
    experiment_name,
    tablename=None,
):
  if tablename is None:
    tablename = f'data/results_tables/{experiment_name}/{res_field}_{secondary_field}'
  else:
    tablename = f'data/results_tables/{experiment_name}/{tablename}'
  query = ''
  query += f'CREATE TABLE sec{hash} AS \n'
  query += f'SELECT *, RANK () OVER (PARTITION BY {group_field},{extract_crit_fields(cases)}\n'
  query += f'ORDER BY {res_field}) AS rank\n'
  query += f'FROM {hash};\n'
  query += f'CREATE TABLE sec{res_field} AS\n'
  query += 'SELECT\n'
  query += f'  {group_field},\n'
  for c in cases[:len(cases) - 1]:
    query += f"  MIN(CASE WHEN {list_to_constraints(c['constraints'])} AND rank=1 THEN {secondary_field} END) AS {c['name']},\n"
  query += f"  MIN(CASE WHEN {list_to_constraints(cases[-1]['constraints'])} AND rank=1 THEN {secondary_field} END) AS {cases[-1]['name']}\n"
  query += f'FROM sec{hash}\n'
  query += f'GROUP BY {group_field};\n'
  query += print_table_to_file('sec' + res_field, tablename)
  query += f'DROP TABLE sec{hash};\n'
  query += f'DROP TABLE sec{res_field};\n'
  return query


def extract_crit_fields(cases):
  crit_fields = []
  for case in cases:
    for c in case['constraints']:
      crit_fields.append(c.split('=')[0])
  crit_fields = set(crit_fields)
  return ','.join(crit_fields)


def generate_table(
    group_field,
    result_fields,
    common_query,
    cases,
    tablenames=None,
    experiment_name=None,
    db_name='data/db.sqlite3',
    secondary=None,
):
  """
  Generates a table in data/results_tables/{experiment_name} for each result_field.
  group_field: The field to group by.
  result_fields: The fields to generate tables for.
  common_query: The common query to filter by.
  cases: The cases to generate columns for (dictionary with 'name', 'constraints' fields). The 'constraints' field is a list of strings.
  tablenames: The names of the tables to generate. If None, the result_fields are used.
  experiment_name: The name of the experiment. If None, 'generic' is used.
  """
  if tablenames is None:
    tablenames = [None] * len(result_fields)
  if experiment_name is None:
    experiment_name = 'generic'
  # Ensure data/results_tables/{experiment_name} exists
  os.system(f'mkdir -p data/results_tables/{experiment_name}')
  query, hash = common_table(common_query)
  for tablename, res_field in zip(tablenames, result_fields):
    query += casewhen_table(
        group_field,
        cases,
        hash,
        res_field,
        experiment_name,
        tablename,
    )
    if secondary is not None:
      if tablename is not None:
        tablename = tablename + f'_{secondary}'
      query += secondary_table(
          group_field,
          secondary,
          cases,
          hash,
          res_field,
          experiment_name,
          tablename,
      )

  query += f'DROP TABLE {hash};\n'
  query_filename = f'{hash}.sql'
  with open(query_filename, 'w') as f:
    f.write(query)
  if not shutil.which('sqlite3'):
    os.system('module load sqlite/intel/3.34.0')
  os.system(f'sqlite3 {db_name} < {query_filename}')
  os.system(f'rm {query_filename}')
