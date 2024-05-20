import sqlite3
import yaml
from hashlib import md5
import sqlite3


def hash_string(given):
  s = str(given)
  return md5(s.encode()).hexdigest()


def add_config(config_dict, filename=None):
  file = dbfile(filename)
  conn = sqlite3.connect(file)
  c = conn.cursor()
  if not 'hash' in config_dict:
    hash = hash_string(config_dict)
    config_dict['hash'] = hash
  keys, values = zip(*config_dict.items())
  keys = ', '.join([*keys])
  values_q = ', '.join(['?']*(len(values)))
  try:
    c.execute(f"INSERT INTO results ({keys}) VALUES ({values_q})", values)
    conn.commit()
    pass
  except Exception as e:
    print(e)
  conn.close()
  pass


def add_result(hash, result_dict, filename=None):
  file = dbfile(filename)
  conn = sqlite3.connect(file)
  c = conn.cursor()
  keys, values = zip(*result_dict.items())
  settings = ', '.join([f"{key} = ?" for key in keys])
  p = f"UPDATE results SET {settings} WHERE hash = ?"
  print(p)
  c.execute(p, [*values, hash])
  conn.commit()
  conn.close()


def fields(fieldfile=None):
  if fieldfile is None:
    fieldfile = 'dbfields.yaml'
  file = fieldfile
  with open(file, 'r') as f:
    fields = yaml.safe_load(f)
  return fields

def config_fields(fieldfile=None):
  c_fields = fields(fieldfile)
  return ', '.join(c_fields[0])


def dict_factory(cursor, row):
    d = {}
    for idx, col in enumerate(cursor.description):
        d[col[0]] = row[idx]
    return d


def dict_to_flags(dict):
  s=''
  for key, value in dict.items():
    s+=(f'--{key}={value} ')
  return s


def generate_commands(base_command, unrun=None, config_filename=None, db_filename=None):
  if unrun is None:
    unrun = 'running=False and failed=False and completed=False and scheduled=False'
  file = dbfile(db_filename)
  conn = sqlite3.connect(file)
  conn.row_factory = dict_factory
  c = conn.cursor()
  # TODO: Make this safe.
  query=f"SELECT {config_fields(config_filename)} FROM results where {unrun}"
  c.execute(query)
  results = c.fetchall()
  conn.close()
  s=''
  for r in results:
    mark_scheduled(r['hash'], db_filename)
    s += base_command + ' ' + dict_to_flags(r) + '\n'
  return s



def mark_all(hash, status, filename=None):
  file = dbfile(filename)
  conn = sqlite3.connect(file)
  c = conn.cursor()
  p = f"UPDATE results SET running=?,scheduled=?,failed=?,completed=? WHERE hash = ?"
  c.execute(p, [*status, hash])
  conn.commit()
  conn.close()


def mark_running(hash, filename=None):
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


if __name__ == "__main__":
  # Simple tets
  cc1 = {'manidim': 20, 'manistyle': 'quad', 'greedy': True}
  cc2 = {'manidim': 20, 'manistyle': 'quad'}
  cc3 = {'manidim': 20, 'manistyle': 'lin'}
  cc4 = {'hash': 'greedy_590', 'manidim': 20, 'manistyle': 'lin'}
  h1 = hash_string(cc1)
  add_config(cc1, 'data/test.db')
  add_config(cc2, 'data/test.db')
  add_config(cc3, 'data/test.db')
  add_config(cc4, 'data/test.db')
  print(generate_commands('python3 run.py', None, None, 'data/test.db'))
  mark_completed(h1)
  pass
