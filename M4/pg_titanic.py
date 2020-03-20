#
## Questions to answer:
#

# How many passengers survived, and how many died?
# How many passengers were in each class?
# How many passengers survived/died within each class?
# What was the average age of survivors vs nonsurvivors?
# What was the average age of each passenger class?
# What was the average fare by passenger class?
    # By survival?
# How many siblings/spouses aboard on average, by passenger class? 
    # By survival?
# How many parents/children aboard on average, by passenger class? 
    # By survival?
# Do any passengers have the same name?
# (Bonus! Hard, may require pulling and processing with Python) 
    # How many married couples were aboard the Titanic? 
    # Assume that two people (one Mr. and one Mrs.) with the same last name 
    # and with at least 1 sibling/spouse aboard are a married couple.

import pandas as pd
import psycopg2
import os
from dotenv import load_dotenv
import json
from psycopg2.extras import execute_values

df = pd.read_csv('https://github.com/LambdaSchool/DS-Unit-3-Sprint-2-SQL-and-Databases/raw/master/module2-sql-for-analysis/titanic.csv')


load_dotenv() 
DB_NAME = os.getenv("DB_NAME", default="OOPS_name")
DB_USER = os.getenv("DB_USER", default="OOPS_user")
DB_PASSWORD = os.getenv("DB_PASSWORD", default="OOPS_password")
DB_HOST = os.getenv("DB_HOST", default="OOPS_host")

connection = psycopg2.connect(dbname=DB_NAME, user=DB_USER, password=DB_PASSWORD, host=DB_HOST)

cursor = connection.cursor()

# 
# TABLE CREATION
# 
titanic_query="""
CREATE TABLE IF NOT EXISTS titanic (
  id INTEGER PRIMARY KEY NOT NULL
  ,survived INTEGER NOT NULL
  ,pclass INTEGER NOT NULL
  ,name TEXT NOT NULL
  ,sex TEXT NOT NULL
  ,age INTEGER NOT NULL
  ,sibling_spouses_aboard INTEGER NOT NULL
  ,parents_children_aboard INTEGER NOT NULL
  ,fare REAL NOT NULL
);
"""
cursor.execute(titanic_query)



# Insert data into titanic table
titanic_insertion_query = """INSERT INTO titanic (id,survived,pclass,name,sex,age,sibling_spouses_aboard,parents_children_aboard,fare)
VALUES %s
ON CONFLICT DO NOTHING
"""
titanic_list = list(df.itertuples(index=True))
execute_values(cursor, titanic_insertion_query, titanic_list)


# ACTUALLY SAVE THE TRANSACTIONS
connection.commit()

# How many passengers survived, and how many died?
survived_query = """
SELECT
    survived
    ,count(survived) as surv_count
FROM
    survival_status
Group BY survived
"""

cursor.execute(survived_query)
survived = cursor.fetchall()
print(f"{survived[1][1]} people survived and {survived[0][1]} people died")

# How many passengers were in each class?
class_query = """
SELECT
    pclass
    ,count(pclass) as class_count
FROM
    attributes
GROUP BY pclass
"""
cursor.execute(class_query)
pclass = cursor.fetchall()
print(f"""{pclass[0][1]} people were in class 1
{pclass[1][1]} people were in class 2
{pclass[2][1]} people were in class 3""")

# How many passengers survived/died within each class?
class_survived_query = """
SELECT
    pclass
    ,count(survived) as survival_count
    ,survived
FROM
    titanic
GROUP BY
    pclass
    ,survived
"""
cursor.execute(class_survived_query)
pclass_survived = cursor.fetchall()
print(pclass_survived)
print(f"""{pclass_survived[0][1]} people survived from class 1
{pclass_survived[3][1]} people died from class 1
{pclass_survived[5][1]} people survived from class 2
{pclass_survived[4][1]} people died from class 2
{pclass_survived[2][1]} people survived from class 3
{pclass_survived[1][1]} people died from class 3""")


# What was the average age of survivors vs nonsurvivors?
age_query = """
SELECT
    survived
    ,AVG(age) as avg_age
FROM
    titanic
GROUP BY survived
"""
cursor.execute(age_query)
age = cursor.fetchall()
print(f"""The average age of survivors was {age[1][1]}
The average age of non-survivors was {age[0][1]}""")


# What was the average age of each passenger class?
pclass_age_query = """
SELECT
    pclass
    ,AVG(age) as avg_age
FROM
    titanic
GROUP BY pclass
"""
cursor.execute(pclass_age_query)
pclass_age = cursor.fetchall()
# print("pclass_age",pclass_age)
print(f"""The average age from class 1 was {pclass_age[0][1]}
The average age from class 2 was {pclass_age[2][1]}
The average age from class 3 was {pclass_age[1][1]}""")

# What was the average fare by passenger class?
fare_pclass_query = """
SELECT
    pclass
    ,AVG(fare) as avg_fare
FROM
    titanic
GROUP BY pclass
"""
cursor.execute(fare_pclass_query)
fare_pclass = cursor.fetchall()
# print("fare_pclass",fare_pclass)
print(f"""The average fare from class 1 was {fare_pclass[0][1]}
The average fare from class 2 was {fare_pclass[2][1]}
The average fare from class 3 was {fare_pclass[1][1]}""")

    # By survival?
fare_surv_query = """
SELECT
    survived
    ,AVG(fare) as avg_fare
FROM
    titanic
GROUP BY survived
"""
cursor.execute(fare_surv_query)
fare_surv = cursor.fetchall()
# print("fare_surv",fare_surv)
print(f"""The average fare of survivors was {fare_surv[1][1]}
The average fare of non-survivors was {fare_surv[0][1]}""")

# How many siblings/spouses aboard on average, by passenger class? 
    # By survival?
avg_sib_spouse_pclass_query = """
SELECT
    pclass
    ,AVG(sibling_spouses_aboard) as avg_sib_spouse
FROM
    titanic
GROUP BY pclass
"""
cursor.execute(avg_sib_spouse_pclass_query)
avg_sib_spouse_pclass = cursor.fetchall()
# print("avg_sib_spouse_pclass",avg_sib_spouse_pclass)
print(f"""The average sibling/spouses aboard in class 1 was {avg_sib_spouse_pclass[0][1]}
The average sibling/spouses aboard in class 2 was {avg_sib_spouse_pclass[2][1]}
The average sibling/spouses aboard in class 3 was {avg_sib_spouse_pclass[1][1]}""")


    # By survival?
avg_sib_spouse_surv_query = """
SELECT
    survived
    ,AVG(sibling_spouses_aboard) as avg_sib_spouse
FROM
    titanic
GROUP BY survived
"""
cursor.execute(avg_sib_spouse_surv_query)
avg_sib_spouse_surv = cursor.fetchall()
# print("avg_sib_spouse_surv",avg_sib_spouse_surv)
print(f"""The average sibling/spouses aboard of survivors was {avg_sib_spouse_surv[1][1]}
The average sibling/spouses aboard of non-survivors was {avg_sib_spouse_surv[0][1]}""")

# breakpoint()
