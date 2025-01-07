import sqlite3
import subprocess

from exp.dict_manipulation import config_fields, config_fields_str, flags_to_dict, dict_to_flags, dict_cartesian_product, keys_vals_sqlite_ready
from exp.dbtools import hash_string, generate_table
import logging

DEFAULT_TIMEOUT = 120

__all__ = [
    "generate_table",
    "flags_to_dict",
    "dict_to_flags",
    "config_fields",
    "hash_string",
    "add_config",
    "generate_commands",
]


def add_config(config_dict, filename=None, timeout=DEFAULT_TIMEOUT):
  file = dbfile(filename)
  conn = sqlite3.connect(file, timeout=timeout)
  c = conn.cursor()
  if not 'hash' in config_dict:
    hash = hash_string(config_dict)
    config_dict['hash'] = hash
  keys, values = keys_vals_sqlite_ready(config_dict)
  keys = ', '.join([*keys])
  values_q = ', '.join(['?'] * (len(values)))
  try:
    c.execute(f"INSERT OR IGNORE INTO results ({keys}) VALUES ({values_q})",
              values)
    conn.commit()
    pass
  except Exception as e:
    raise e
  conn.close()
  pass


def add_configs(config_dicts, filename=None, timeout=DEFAULT_TIMEOUT):
  """
    Add multiple configurations to the SQLite database in a single transaction.

    Parameters:
    - config_dicts (list): List of configuration dictionaries to add.
    - filename (str): SQLite database file name.
    - timeout (int): SQLite connection timeout.
    """
  file = dbfile(filename)
  conn = sqlite3.connect(file, timeout=timeout)
  c = conn.cursor()

  # Prepare all inserts in a single transaction
  try:
    for config_dict in config_dicts:
      # Ensure 'hash' key exists
      if 'hash' not in config_dict:
        config_dict['hash'] = hash_string(config_dict)

      # Prepare keys and values
      keys, values = keys_vals_sqlite_ready(config_dict)
      keys = ', '.join([*keys])
      values_q = ', '.join(['?'] * len(values))

      # Execute the INSERT statement
      c.execute(f"INSERT OR IGNORE INTO results ({keys}) VALUES ({values_q})",
                values)

    # Commit all changes in one transaction
    conn.commit()
  except Exception as e:
    conn.rollback()
    raise e
  finally:
    conn.close()


def add_result(hash, result_dict, filename=None, timeout=DEFAULT_TIMEOUT):
  file = dbfile(filename)
  conn = sqlite3.connect(file, timeout=timeout)
  c = conn.cursor()
  keys, values = keys_vals_sqlite_ready(result_dict)
  settings = ', '.join([f"{key} = ?" for key in keys])
  p = f"UPDATE results SET {settings} WHERE hash = ?"
  c.execute(p, [*values, hash])
  conn.commit()
  conn.close()


def dict_factory(cursor, row):
  d = {}
  for idx, col in enumerate(cursor.description):
    d[col[0]] = row[idx]
  return d


def evaluate(base_command, config_dict):
  if callable(base_command):
    return base_command(config_dict)
  else:
    return base_command


def generate_commands(base_command,
                      config_filename=None,
                      db_filename=None,
                      rerun_failed=False,
                      rerun_scheduled=False,
                      rerun_completed=False,
                      suffix='',
                      timeout=DEFAULT_TIMEOUT,
                      extra_conditions=None):
  statements = ['running=False']
  if not rerun_failed:
    statements.append('failed=False')
  if not rerun_scheduled:
    statements.append('scheduled=False')
  if not rerun_completed:
    statements.append('completed=False')
  unrun = ' AND '.join(statements)
  file = dbfile(db_filename)
  conn = sqlite3.connect(file, timeout=timeout)
  conn.row_factory = dict_factory
  c = conn.cursor()
  # TODO: Make this safe.
  query = f"SELECT {config_fields_str(config_filename)} FROM results where {unrun}"
  if extra_conditions is not None:
    query += f" AND {' AND '.join(extra_conditions)}"
  c.execute(query)
  results = c.fetchall()
  try:
    for r in results:
      c.execute("UPDATE results SET scheduled=True WHERE hash=?", (r['hash'],))
    conn.commit()
  except Exception as e:
    conn.rollback()
    raise e
  finally:
    conn.close()

  # Generate commands
  commands = []
  for r in results:
    command = evaluate(base_command, r) + ' ' + dict_to_flags(r) + ' ' + suffix
    commands.append(command)

  return '\n'.join(commands)


def mark_all(hash, status, filename=None, timeout=DEFAULT_TIMEOUT):
  file = dbfile(filename)
  conn = sqlite3.connect(file, timeout=timeout)
  c = conn.cursor()
  p = f"UPDATE results SET running=?,scheduled=?,failed=?,completed=? WHERE hash = ?"
  c.execute(p, [*status, hash])
  conn.commit()
  conn.close()


def set_git_hash(hash, git_hash, filename=None, timeout=DEFAULT_TIMEOUT):
  file = dbfile(filename)
  conn = sqlite3.connect(file, timeout=timeout)
  c = conn.cursor()
  p = f"UPDATE results SET git_hash=? WHERE hash = ?"
  c.execute(p, [git_hash, hash])
  conn.commit()
  conn.close()


def get_git_hash():
  try:
    # Run the Git command to get the current commit hash
    git_hash = subprocess.check_output(['git', 'rev-parse',
                                        'HEAD']).strip().decode('utf-8')
    return git_hash
  except subprocess.CalledProcessError:
    return "Not a Git repository or no commits"


def is_git_dirty():
  # Check if there are any uncommitted changes
  result = subprocess.run(['git', 'status', '--porcelain'],
                          stdout=subprocess.PIPE,
                          stderr=subprocess.PIPE,
                          text=True)

  # If the result has any output, the repo is dirty
  return bool(result.stdout)


def mark_running(hash, filename=None, run_when_dirty=False):
  if is_git_dirty():
    if run_when_dirty:
      logging.warning('Git repo is dirty, but run_when_dirty is True'
                      'Running experiment anyway; this is not recommended.')
    else:
      raise RuntimeError(
          'The Git repository has uncommitted changes. Not running experiment')
  set_git_hash(hash, get_git_hash(), filename)
  mark_all(hash, [True, False, False, False], filename)


def mark_scheduled(hash, filename=None):
  mark_all(hash, [False, True, False, False], filename)


def mark_failed(hash, filename=None):
  mark_all(hash, [False, False, True, False], filename)


def mark_completed(hash, filename=None):
  mark_all(hash, [False, False, False, True], filename)


def dbfile(filename):
  if filename is None:
    filename = 'data/db.sqlite3'
  return filename


def main(_):
  # Simple tets
  cc1 = {
      'manidim': [20, 25],
      'manistyle': ['quad', 'lin'],
      'greedy': [True, False],
      'opti.test': [20],
  }
  cc2 = {'hash': 'greedy_590', 'manidim': 20, 'manistyle': 'lin'}
  cc3 = {'hash': 'should_not_appear', 'manidim': 39, 'manistyle': 'lin'}
  cc4 = {'hash': 'greedy_filteredout', 'manidim': 50, 'manistyle': 'lin'}
  cc5 = {
      'hash': 'vecinput',
      'manidim': 20,
      'manistyle': 'lin',
      'init_idcs': "[1, 2, 3]"
  }
  dd = dict_cartesian_product(**cc1)
  h1 = hash_string(cc1)
  for d in dd:
    print(d)
    add_config(d, 'data/test.db')
  add_config(cc2, 'data/test.db')
  add_config(cc3, 'data/test.db')
  add_configs([cc4, cc5], 'data/test.db')

  mark_running('should_not_appear', 'data/test.db')

  def command(config):
    s = 'python3 run.py '
    if config['manidim'] == 20:
      s += '--computation_time=short '
    return s

  print(
      generate_commands(
          command,
          'data/dbfields.yaml',
          'data/test.db',
          suffix='--dry-run',
          extra_conditions=['manidim<45']))
  mark_completed(h1, 'data/test.db')


if __name__ == "__main__":
  main(None)
