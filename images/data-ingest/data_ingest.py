import pandas as pd
import sqlalchemy

# MySQL setups
mysql_engine = sqlalchemy.create_engine("mysql+pymysql://codetest:swordfish@database:3306/codetest")
conxn = mysql_engine.connect()

# CSV Data in
ppl = pd.read_csv('data/people.csv')
places = pd.read_csv('data/places.csv')

# Countries
# Getting unique countries to insert into the countries table
input_countries = pd.DataFrame({
                                #'country_id': '', # ID to check against existing in the db
                                'country_name': places['country'].unique().tolist(),
                            })

# --- this could be a func ---
# Check existing
existing_countries = pd.read_sql_table('countries', con=conxn,)
input_countries = input_countries.merge(existing_countries, how='left', on='country_name')

# Keep nonexistent
input_countries = input_countries[input_countries['country_id'].isna()]

# Input nonexistent if any
if not input_countries.empty:
    input_countries[['country_name']].to_sql('countries', con=conxn, if_exists='append', index=False)

# Regions
# Matching the regions to country_id from the countries table
input_regions = places[['country','county']].drop_duplicates()

# Renaming
input_regions.rename(columns={'country':'country_name', 'county':'region_name'}, inplace=True)

# Countries table fetch and merge
countries = pd.read_sql_table('countries', con=conxn,)
input_regions = input_regions.merge(countries, how='left', on=['country_name'])

# Check existing
existing_regions = pd.read_sql_table('regions', con=conxn,)
input_regions = input_regions.merge(existing_regions, how='left', on=['region_name', 'country_id'])

# Keep nonexistent
input_regions = input_regions[input_regions['region_id'].isna()]

# Input nonexistent if any
if not input_regions.empty:
    input_regions[['region_name', 'country_id']].to_sql('regions', con=conxn, if_exists='append', index=False)

# Cities
# Matching the cities to country_id from the countries table
input_cities = places[['county', 'city']].drop_duplicates()

# Renaming
input_cities.rename(columns={'county':'region_name', 'city':'city_name'}, inplace=True)

# Countries & Region table fetch and merge
regions = pd.read_sql_table('regions', con=conxn,)
input_cities = input_cities.merge(regions[['region_name', 'region_id']], how='left', on=['region_name'])

# Check existing
existing_cities = pd.read_sql_table('cities', con=conxn,)
input_cities = input_cities.merge(existing_cities, how='left', on=['city_name','region_id'])

# Keep nonexistent
input_cities = input_cities[input_cities['city_id'].isna()]

# Input nonexistent if any
if not input_cities.empty:
    input_cities[['city_id', 'region_id', 'city_name']].to_sql('cities', con=conxn, if_exists='append', index=False)

# People
# Merging city_id from cities table to ppl
cities = pd.read_sql_table('cities', con=conxn)
input_ppl = ppl.merge(cities, how='left', left_on=['place_of_birth'], right_on=['city_name'])

# Gathering people born in cities not yet in the db
new_city = input_ppl[input_ppl['city_id'].isna()]

# Inseting all people not yet present in the db
existing_ppl = pd.read_sql_table('people', con=conxn,)
input_ppl = input_ppl.merge(existing_ppl, how='left', on=['given_name', 'family_name', 'date_of_birth', 'city_id'])

# Keep nonexistent
input_ppl = input_ppl[input_ppl['personal_id'].isna()]

# Input nonexistent if any
if not input_ppl.empty:
    input_ppl[['given_name', 'family_name', 'date_of_birth', 'city_id']].to_sql('people', con=conxn, if_exists='append', index=False)