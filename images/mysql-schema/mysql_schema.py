#!/usr/bin/env python

import sqlalchemy
from sqlalchemy import Table, Column, Integer, String, Date, ForeignKey

# MySQL setups
mysql_engine = sqlalchemy.create_engine("mysql+pymysql://codetest:swordfish@database:3306/codetest")
conxn = mysql_engine.connect()

# Schema
# Drop all as the intended behaviour
drop_meta = sqlalchemy.MetaData()
drop_meta.reflect(bind=mysql_engine)
drop_meta.drop_all(bind=mysql_engine)

#Create
metadata = sqlalchemy.MetaData()
metadata.reflect(bind=mysql_engine)

# Countries
countries = Table(
                'countries',
                metadata,
                Column('country_id', Integer, primary_key=True, autoincrement=True),
                Column('country_name', String(50), nullable=False),
            )

# Region = County, (county and country is too similiar so using a synonym)
regions = Table(
                'regions',
                metadata,
                Column('region_id', Integer, primary_key=True, autoincrement=True),
                Column('country_id', Integer, ForeignKey('countries.country_id'), nullable=False),
                Column('region_name', String(100), nullable=False),
            )

# Cities
cities = Table(
                'cities',
                metadata,
                Column('city_id', Integer, primary_key=True, autoincrement=True),
                Column('region_id', Integer, ForeignKey('regions.region_id'), nullable=False),
                Column('city_name', String(100), nullable=False),

            )

# People
people = Table(
                'people',
                metadata,
                Column('personal_id', Integer, primary_key=True, autoincrement=True),
                Column('given_name', String(100)),
                Column('family_name', String(100)),
                Column('date_of_birth', Date),
                Column('city_id', Integer, ForeignKey('cities.city_id'), nullable=False),
            )


# Creating the tables
metadata.create_all(mysql_engine)
