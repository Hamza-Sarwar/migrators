import pandas as pd
import psycopg2
from sqlalchemy import create_engine

from config import *

df_inventory = pd.read_excel('data/InventoryLog.xlsx')
df_color = pd.read_excel('data/InvColors.xlsx', skiprows=1)
df_body = pd.read_excel('data/BodyTypes.xlsx')
df_price = pd.read_excel('data/Valuation.xlsx')


df_inventory = df_inventory[['ID', 'VIN', 'Stock No', 'StoreId', 'ActualLocation', 'Year', 'Make', 'Model',
                             'BodyTypeId', 'InteriorColorId', 'ExteriorColorId', 'Doors', 'Cylynders',
                             'AvailabilityId', 'IsLuxury', 'VehiLink']]

df_color = df_color[['Id.1', 'Name.1']]
df_color.columns = ['ExteriorColorId', 'ExteriorColorName']

df_body = df_body[['ID', 'Name']]
df_body.columns = ['BodyTypeId', 'BodyType']

df_price = df_price[['ID', 'InventoryId.1', 'Price1']]
df_price.columns = ['ID', 'InventoryId', 'Price']


df_inventory = pd.merge(df_inventory, df_color, on='ExteriorColorId', how='left')
df_inventory = pd.merge(df_inventory, df_body, on='BodyTypeId', how='left')
df_inventory = pd.merge(df_inventory, df_price, left_on='ID', right_on='InventoryId', how='left')


df_inventory.drop(columns=['InventoryId', 'BodyTypeId', 'InteriorColorId', 'ExteriorColorId', 'ID_y'], inplace=True)
df_inventory.rename(columns={'ID_x': 'ID'}, inplace=True)
df_inventory['IsLuxury'] = df_inventory['IsLuxury'].astype(bool)


pg_conn = psycopg2.connect(host=HOST, dbname=DBNAME, user=USER, password=PASSWORD)
pg_cursor = pg_conn.cursor()

drop_table_query = "DROP TABLE IF EXISTS inventory;"
pg_cursor.execute(drop_table_query)
pg_conn.commit()

create_table_query = """
CREATE TABLE inventory (
    "ID" INT PRIMARY KEY,
    "VIN" VARCHAR(255),
    "Stock No" VARCHAR(255),
    "StoreId" INT,
    "ActualLocation" VARCHAR(255),
    "Year" INT,
    "Make" VARCHAR(255),
    "Model" VARCHAR(255),
    "Doors" INT,
    "Cylynders" INT,
    "AvailabilityId" INT,
    "IsLuxury" BOOLEAN,
    "VehiLink" VARCHAR(255),
    "ExteriorColorName" VARCHAR(255),
    "BodyType" VARCHAR(255),
    "Price" FLOAT
);
"""
pg_cursor.execute(create_table_query)
pg_conn.commit()

engine = create_engine(f'postgresql://{USER}:{PASSWORD}@{HOST}/{DBNAME}')
df_inventory.to_sql('inventory', engine, if_exists='append', index=False)

pg_cursor.close()
pg_conn.close()
