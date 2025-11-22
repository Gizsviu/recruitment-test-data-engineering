import pandas as pd
import sqlalchemy
import json

# MySQL setups
mysql_engine = sqlalchemy.create_engine("mysql+pymysql://codetest:swordfish@database:3306/codetest")
conxn = mysql_engine.connect()

# Predefined query read into a df
with open('ppl_per_country.sql', 'r') as q:
    query = q.read()
ppl_per_country = pd.read_sql(query, con=conxn)

# Orient record into a dict &  Ouput into a JSON file
result = ppl_per_country.set_index('Country')['Population'].to_dict()
with open('data/summary_output.json', 'w') as f:
    json.dump(result, f)