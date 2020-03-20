#
## Questions to answer:
#

# How many total Characters are there?
# How many of each specific subclass?
# How many total Items?
# How many of the Items are weapons? How many are not?
# How many Items does each character have? (Return first 20 rows)
# How many Weapons does each character have? (Return first 20 rows)
# On average, how many Items does each Character have?
# On average, how many Weapons does each character have?

import pymongo
import os
from dotenv import load_dotenv
import json
import urllib.request
import sqlite3

## Connection to Mongo DB ##

# Load in credentials and connect to MongoDB
load_dotenv()
DB_USER = os.getenv("MONGO_USER", default="OOPS")
DB_PASSWORD = os.getenv("MONGO_PASSWORD", default="OOPS")
CLUSTER_NAME = os.getenv("MONGO_CLUSTER_NAME1", default="OOPS")

connection_uri = f"mongodb+srv://{DB_USER}:{DB_PASSWORD}@{CLUSTER_NAME}.mongodb.net/test?retryWrites=true&w=majority"
# print("----------------")
# print("URI:", connection_uri)

client = pymongo.MongoClient(connection_uri)
# print("----------------")
# print("CLIENT:", type(client), client)

# Create a database
db = client.rpg_database 
# print("----------------")
# print("DB:", type(db), db)

## Connection to SQLite DB ##

# Save the sqlite filepath for the DB to a variable
DB_FILEPATH = os.path.join(os.path.dirname(__file__), "..","M1/rpg_db.sqlite3")

# Intanstiate the connection
lite_connection = sqlite3.connect(DB_FILEPATH)

# Instantiate the cursor
lite_cursor = lite_connection.cursor()

## TABLE: charactercrator_creator

# Take the first table, character, out of the sqlite DB
char_query = """
SELECT
    *
FROM
    charactercreator_character
"""
char_result = lite_cursor.execute(char_query).fetchall()
# Save a tuple of all the column names
char_columns = ('character_id','name','level','exp','hp','strength','intelligence','dexterity','wisdom')
# Create a dictionary with the correct keys and values
char_dict = [dict(zip(char_columns,values)) for values in char_result]
# Store in MondgoDB
collection = db.charactercreator_character
collection.insert_many(char_dict)

## TABLE: armory_item

# Take armory_item table out of the sqlite DB
item_query = """
SELECT
    *
FROM
    armory_item
"""
item_result = lite_cursor.execute(item_query).fetchall()
# Save a tuple of all the column names
item_columns = ('item_id','name','value','weight')
# Create a dictionary with the correct keys and values
item_dict = [dict(zip(item_columns,values)) for values in item_result]
# Store in MondgoDB
collection = db.armory_item
collection.insert_many(item_dict)

## TABLE: armory_weapon

# Take armory_weapon table out of the sqlite DB
weapon_query = """
SELECT
    *
FROM
    armory_weapon
"""
weapon_result = lite_cursor.execute(weapon_query).fetchall()
# Save a tuple of all the column names
weapon_columns = ('weapon_id','power')
# Create a dictionary with the correct keys and values
weapon_dict = [dict(zip(weapon_columns,values)) for values in weapon_result]
# Store in MondgoDB
collection = db.armory_weapon
collection.insert_many(weapon_dict)

## TABLE: charactercreator_character_inventory

# Take charactercreator_character_inventory table out of the sqlite DB
inv_query = """
SELECT
    *
FROM
    charactercreator_character_inventory
"""
inv_result = lite_cursor.execute(inv_query).fetchall()
# Save a tuple of all the column names
inv_columns = ('id','character_id','item_id')
# Create a dictionary with the correct keys and values
inv_dict = [dict(zip(inv_columns,values)) for values in inv_result]
# Store in MondgoDB
collection = db.charactercreator_character_inventory
collection.insert_many(inv_dict)

## TABLE: charactercreator_mage

# Take charactercreator_mage table out of the sqlite DB
mage_query = """
SELECT
    *
FROM
    charactercreator_mage
"""
mage_result = lite_cursor.execute(mage_query).fetchall()
# Save a tuple of all the column names
mage_columns = ('character_ptr_id','has_pet','mana')
# Create a dictionary with the correct keys and values
mage_dict = [dict(zip(mage_columns,values)) for values in mage_result]
# Store in MondgoDB
collection = db.charactercreator_mage
collection.insert_many(mage_dict)

## TABLE: charactercreator_thief

# Take charactercreator_thief table out of the sqlite DB
thief_query = """
SELECT
    *
FROM
    charactercreator_thief
"""
thief_result = lite_cursor.execute(thief_query).fetchall()
# Save a tuple of all the column names
thief_columns = ('character_ptr_id','is_sneaking','energy')
# Create a dictionary with the correct keys and values
thief_dict = [dict(zip(thief_columns,values)) for values in thief_result]
# Store in MondgoDB
collection = db.charactercreator_thief
collection.insert_many(thief_dict)

## TABLE: charactercreator_cleric

# Take charactercreator_cleric table out of the sqlite DB
cleric_query = """
SELECT
    *
FROM
    charactercreator_cleric
"""
cleric_result = lite_cursor.execute(cleric_query).fetchall()
# Save a tuple of all the column names
cleric_columns = ('character_ptr_id','using_shield','mana')
# Create a dictionary with the correct keys and values
cleric_dict = [dict(zip(cleric_columns,values)) for values in cleric_result]
# Store in MondgoDB
collection = db.charactercreator_cleric
collection.insert_many(cleric_dict)

## TABLE: charactercreator_fighter

# Take charactercreator_fighter table out of the sqlite DB
fighter_query = """
SELECT
    *
FROM
    charactercreator_fighter
"""
fighter_result = lite_cursor.execute(fighter_query).fetchall()
# Save a tuple of all the column names
fighter_columns = ('character_ptr_id','using_shield','rage')
# Create a dictionary with the correct keys and values
fighter_dict = [dict(zip(fighter_columns,values)) for values in fighter_result]
# Store in MondgoDB
collection = db.charactercreator_fighter
collection.insert_many(fighter_dict)

# How many total Characters are there?
character_count = db.charactercreator_character.count()
print(f"There are {character_count} characters")

# How many of each specific subclass?
mage_count = db.charactercreator_mage.count()
thief_count = db.charactercreator_thief.count()
cleric_count = db.charactercreator_cleric.count()
fighter_count = db.charactercreator_fighter.count()
print(f"There are {mage_count} mages")
print(f"There are {thief_count} thieves")
print(f"There are {cleric_count} clerics")
print(f"There are {fighter_count} fighters")

# How many total Items?
item_count = db.armory_item.count()
print(f"There are {item_count} items")

# How many of the Items are weapons? How many are not?
weapon_count = db.armory_weapon.count()
print(f"There are {weapon_count} weapons")
print(f"Of the items, {item_count-weapon_count} are not weapons")

