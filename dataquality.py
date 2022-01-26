import enum
from http.client import REQUEST_ENTITY_TOO_LARGE
from operator import le
from xmlrpc.client import boolean
import requests
import os
import pandas as pd
import json
import re
from datetime import datetime

'''
string = str
number = float
boolean = bool
array = array
'''

correspondance = {
  "string": "str",
  "number": "float",
  "boolean": "bool",
  "array": "list"
}

df_report = pd.DataFrame(columns=['row_number', 'fields'])

def validate_date(d, dateformat):
  try:
    datetime.strptime(d, dateformat.values[0])
    return True
  except ValueError:
    return False

def validate_regex(data, reg):
  # print(type(reg.values[0]))
  # print(data)
  x = re.search(reg.values[0], str(data))
  return x

def calculate_dataquality_of_file(df_schema, df_data, df_report):
  # print(df_data.head())
  err_rows = 0
  correct_rows = 0
  usable_rows = 0

  # flags
  error = 0
  usable = 0
  row_count = 0
  # print(df_schema.head())
  # print(len(df_schema))

  # get list of columns
  # plutot iterer sur le df_data

  # ici faire le for pour toutes les lignes de df_data
  for y in range(len(df_data)):
    error = 0
    usable = 0
    row_count += 1
    fields_err_list = []
    for i in range(len(df_schema)):
      # print(df_schema['field'][i])
      field = df_schema['field'][i]
      required = df_schema['required'][i]
      format = df_schema['format'][i]
      pattern = df_schema['pattern'][i]
      typed = df_schema['type'][i]
      enumd = df_schema['enum'][i]
      # print(required)
      # print(df_data[field][y])

      # check if required is answered (if there is no pattern of format check if there is data)
      # if not err_rows + 1
      if required == True:
        if pd.isna(df_data[field][y]) or pd.isnull(df_data[field][y]):
          print("is empty error")
          error += 1
          fields_err_list.append(field)
        elif format != None:
          # check if there is error in data format
          if format == 'email':
            # this is a regex to check email validity
            x = re.search("(^[a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\.[a-zA-Z0-9-.]+$)", str(df_data[field][y]))
            if x == None:
              print("email format error error")
              error += 1
              fields_err_list.append(field)
          if typed == 'date':
            tmp_df = df_schema[df_schema['field'] == field]
            # print(tmp_df.head())
            vald = validate_date(df_data[field][y], tmp_df['format'])
            if vald == False:
              print("date format error")
              error += 1
              fields_err_list.append(field)
        elif pattern != None:
          tmp_df = df_schema[df_schema['field'] == field]
          # print(tmp_df.head())
          res = validate_regex(df_data[field][y], tmp_df['pattern'])
          # print(res)
          # print(df_data[field][y])
          tmp_df['pattern']
          # print(field)
          # print(df_data[field][y])
          if res == None:
            error += 1
            print("regex error")
            fields_err_list.append(field)
        elif enumd != None:
          # print("enum values : " + str(enumd))
          # print("value : " + str(df_data[field][y]))
          enum_set = frozenset(enumd)
          if df_data[field][y] not in enum_set:
            error += 1
            print('error enum')
            fields_err_list.append(field)
        elif typed == "number":
          try:
            tmp = float(df_data[field][y])
          except:
            tmp = df_data[field][y]

          r = isinstance(tmp, float)
          if r == False: 
            error += 1
            print("number value error")
            fields_err_list.append(field)
        elif typed == "integer":
          try:
            tmp = int(df_data[field][y])
          except:
            tmp = df_data[field][y]
          r = isinstance(tmp, int)
          if r == False:
            error += 1
            print('integer error')
            fields_err_list.append(field)
        elif typed == "boolean":
          # print(field)
          tmp = str(df_data[field][y]).capitalize()
          try:
            v = bool(tmp)
          except:
            v = df_data[field][y]
          r = isinstance(v, bool)
          if r == False:
            error += 1
            print("bool value error")
            fields_err_list.append(field)
        elif typed == "geopoint":
          r = isinstance(df_data[field][y], list)
          if not any(isinstance(item, float) for item in df_data[field][y]):
            iu = False
          else:
            iu = True
          if r == False or iu == False:
            print("geopoint value error")
            error += 1
            fields_err_list.append(field)
      else:
        if pd.isna(df_data[field][y]) or pd.isnull(df_data[field][y]):
          print("is empty error")
          usable += 1
          fields_err_list.append(field)
        elif format != None:
          # check if there is error in data format
          if format == 'email':
            # this is a regex to check email validity
            x = re.search("(^[a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\.[a-zA-Z0-9-.]+$)", str(df_data[field][y]))
            if x == False:
              print("email format error error")
              usable += 1
              fields_err_list.append(field)
          if typed == 'date':
            tmp_df = df_schema[df_schema['field'] == field]
            # print(tmp_df.head())
            vald = validate_date(df_data[field][y], tmp_df['format'])
            if vald == False:
              print("date format error")
              usable += 1
              fields_err_list.append(field)
        elif pattern != None:
          tmp_df = df_schema[df_schema['field'] == field]
          # print(tmp_df.head())
          res = validate_regex(df_data[field][y], tmp_df['pattern'])
          # print(res)
          # print(df_data[field][y])
          tmp_df['pattern']
          # print(field)
          # print(df_data[field][y])
          if res == None:
            usable += 1
            print("regex error")
            fields_err_list.append(field)
        elif enumd != None:
          # print("enum values : " + str(enumd))
          # print("value : " + str(df_data[field][y]))
          enum_set = frozenset(enumd)
          if df_data[field][y] not in enum_set:
            usable += 1
            print('error enum')
            fields_err_list.append(field)
        elif typed == "number":
          try:
            tmp = float(df_data[field][y])
          except:
            tmp = df_data[field][y]

          r = isinstance(tmp, float)
          if r == False: 
            usable += 1
            print("number value error")
            fields_err_list.append(field)
        elif typed == "integer":
          try:
            tmp = int(df_data[field][y])
          except:
            tmp = df_data[field][y]
          r = isinstance(tmp, int)
          if r == False:
            usable += 1
            print('integer error')
            fields_err_list.append(field)
        elif typed == "boolean":
          # print(field)
          tmp = str(df_data[field][y]).capitalize()
          try:
            v = bool(tmp)
          except:
            v = df_data[field][y]
          r = isinstance(v, bool)
          if r == False:
            usable += 1
            print("bool value error")
            fields_err_list.append(field)
        elif typed == "geopoint":
          r = isinstance(df_data[field][y], list)
          if not any(isinstance(item, float) for item in df_data[field][y]):
            iu = False
          else:
            iu = True
          if r == False or iu == False:
            print("geopoint value error")
            usable += 1
            fields_err_list.append(field)

      # check if required is answered (if there is a pattern or format check if there is data and the constraints are answered)
      # if not err_rows + 1
    # print(error)
    # print(usable)
    df_report_tmp = pd.DataFrame([[row_count, fields_err_list]], columns=['row_number', 'fields'])
    df_report = df_report.append(df_report_tmp, ignore_index=True)
    if error > 0:
      err_rows += 1
    elif usable > 0:
      usable_rows += 1
    else:
      correct_rows += 1

  print("error row : " + str(err_rows))
  print("usable row : " + str(usable_rows + correct_rows))
  print("correct row : " + str(correct_rows))
  print("##########################################\nResults:\n- percentage of correct rows : {}\n- percentage of usable rows : {}\n- percentage of error rows : {}".format((correct_rows/row_count)*100, ((usable_rows+correct_rows)/row_count)*100, (err_rows/row_count)*100))
  return df_report

def read_csv_file(file_path):
  df = pd.read_csv(file_path)
  return df

def get_madatory_columns(schema_url):
  df_schema = pd.DataFrame(columns=['field', 'required', 'format', 'pattern', 'type', 'enum'])
  schema_raw = requests.get(schema_url)
  # print(schema_raw.json())
  schema_r = schema_raw.json()
  # print(schema_r['fields'])
  # print(len(schema_r['fields']))
  len_fields = len(schema_r['fields'])
  # print(type(schema_r))

  for i in range(len_fields):
    field = schema_r['fields'][i]['name']
    mandatory_flag = schema_r['fields'][i]['constraints']['required']
    typed = schema_r['fields'][i]['type']
    # format = schema_r.get("fields", []).get(i, [])
    # print(format)

    try:
      format = schema_r["fields"][i]["format"]
      # print(format)
    except:
      format = None

    try:
      pattern = schema_r["fields"][i]["constraints"]["pattern"]
      # print(pattern)
    except:
      pattern = None

    try:
      enumd = schema_r["fields"][i]["constraints"]["enum"]
      # print(pattern)
    except:
      enumd = None

    #print("{} : {} : {}".format(field, mandatory_flag, format))
    df_temp = pd.DataFrame([[field, mandatory_flag, format, pattern, typed, enumd]], columns=['field', 'required', 'format', 'pattern', 'type', 'enum'])
    df_schema = df_schema.append(df_temp, ignore_index=True)
  
  return df_schema

x = requests.get('https://www.data.gouv.fr/api/1/datasets/schemas')
schemas = x.json()

schema = ''

for item in schemas:
  if item['id'] == 'etalab/schema-irve':
    schema = item

#print(schema)
name = schema['id']
versions = schema['versions']

# print(name)
# print(versions)

flag = False
schema_url = ''

while flag == False:
  for i in versions:
    print('version: ' + i)
  print('these versions are available, enter the one you want:')
  x = input()
  if x in versions:
    flag = True
    print('the version selected is ' + x)
    schema_url = 'https://schema.data.gouv.fr/schemas/{}/{}/schema.json'.format(name, x)
  else:
    print('please enter an correct version number')

print("please enter the path to the file you want to analyze: ")
file_path = input()
if os.path.isfile(file_path):
  print("the file exists")
  df = read_csv_file(file_path)
  # print(df.head())
else:
  print("the file can't be found please check the path you entered")

df_schema = get_madatory_columns(schema_url)
# print(df_schema.head())
# df_schema.to_excel('schema2.xlsx', sheet_name='schema')
df_report = calculate_dataquality_of_file(df_schema, df, df_report)
# print(df_report.head())
df_report.to_excel('report.xlsx', sheet_name='report')

