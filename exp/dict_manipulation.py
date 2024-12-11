"""Tools parsing dicts into flags and vice versa"""
from enum import Enum
import itertools


def keys_vals_sqlite_ready(d):
  keys, values = zip(*replace_lists_with_strings(d).items())
  fixed_keys = []
  for key in keys:
    if '.' in key:
      fixed_keys.append('\"' + key + '\"')
    else:
      fixed_keys.append(key)
  return fixed_keys, values


def replace_lists_with_strings(d):
  for key, value in d.items():
    if isinstance(value, list):
      d[key] = str(value)
  return d


def config_fields(fieldfile=None):
  if fieldfile is None:
    fieldfile = 'dbfields'
  with open(fieldfile, 'r', encoding='utf-8') as file:
    cfg_fields = file.read().splitlines()
  return filter_commented_or_empty(cfg_fields)


def filter_commented_or_empty(fields: list):
  out_fields = []
  for field in fields:
    if (len(field) > 0) and (field[0] != "#"):
      if '.' in field:
        out_fields.append('\"' + field + '\"')
      else:
        out_fields.append(field)
  return out_fields


def config_fields_str(fieldfile=None):
  return ', '.join(config_fields(fieldfile))


def flags_to_dict(flags, flags_to_read=None):
  if flags_to_read is None:
    flags_to_read = config_fields()
  config_dict = {}
  for flagname in flags_to_read:
    flagval = getattr(flags, flagname)
    if isinstance(flagval, Enum):
      flagval = flagval.name
    config_dict[flagname] = flagval
  return config_dict


def dict_to_flags(dict):
  s = ''
  for key, value in dict.items():
    if value is not None:
      if type(value) is str:
        if len(value) > 0:
          if value[0] == '[':
            array_entries = value[1:-1].split(', ')
            for entry in array_entries:
              s += (f'--{key}={entry} ')
          else:
            s += (f'--{key}={value} ')
      else:
        s += (f'--{key}={value} ')
  return s


def dict_cartesian_product(**kwargs):
  keys = kwargs.keys()
  for instance in itertools.product(*kwargs.values()):
    yield dict(zip(keys, instance))
