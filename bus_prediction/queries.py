import os
from dotenv import load_dotenv
import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy import text

def load_and_process_data(file_path):
    data = pd.read_json(file_path, encoding='latin-1') 

    data['latitude'] = data['latitude'].str.replace(',', '.').astype(float)
    data['longitude'] = data['longitude'].str.replace(',', '.').astype(float)
    data['velocidade'] = data['velocidade'].astype(int)
  
    return data

def process_folder(folder_path, engine):
    for root, _, files in os.walk(folder_path):
        for file in files:
            file_path = os.path.join(root, file)
            print(f"Processing file: {file_path}")
            data = load_and_process_data(file_path)
            data.to_sql('bus_positions', engine, if_exists='append', index=False, method='multi')

load_dotenv()
database_url = os.getenv("DATABASE_URL")
engine = create_engine(database_url, client_encoding='latin-1')  

# create_table_query = """
# CREATE TABLE IF NOT EXISTS bus_positions (
#     ordem VARCHAR(20),
#     latitude NUMERIC(9, 6),
#     longitude NUMERIC(9, 6),
#     datahora BIGINT,
#     velocidade SMALLINT,
#     linha VARCHAR(10),
#     datahoraenvio BIGINT,
#     datahoraservidor BIGINT,
#     datahora_ts TIMESTAMP,
#     datahoraenvio_ts TIMESTAMP,
#     datahoraservidor_ts TIMESTAMP
# );
# """
# with engine.connect() as conn:
#     conn.execute(text(create_table_query))
        
# folder_path = "/Users/casalecchi/Code/DataMining/data/bus/gps"
# process_folder(folder_path, engine)

# add_id_column_query = """
# ALTER TABLE bus_positions
# ADD COLUMN id SERIAL PRIMARY KEY;
# """

# with engine.connect() as conn:
#     conn.execute(text(add_id_column_query))
#     conn.commit()

# create_view_query = """
# CREATE MATERIALIZED VIEW bus_positions_filtered_materialize AS
# SELECT  
#         row_number() OVER () AS id,
#         ordem, 
#         latitude,
#         longitude,
# 		ST_SetSRID(ST_MakePoint(longitude, latitude), 4326)::geography as geom,
#         TO_TIMESTAMP(datahora / 1000) as datahora,
#         velocidade,
#         linha,
#         TO_TIMESTAMP(datahoraservidor / 1000) as datahoraservidor
        
# FROM bus_positions
# WHERE EXTRACT(HOUR FROM TO_TIMESTAMP(datahoraservidor / 1000)) BETWEEN 4 AND 23
# AND linha IN ('483', '864', '639', '3', '309', '774', '629', '371', '397', '100', '838', '315', '624', '388', '918', '665', '328', '497', '878', '355', '138', '606', '457', '550', '803', '917', '638', '2336', '399', '298', '867', '553', '565', '422', '756', '186012003', '292', '554', '634', '232', '415', '2803', '324', '852', '557', '759', '343', '779', '905', '108');
# """

# add_index_query = """
# CREATE INDEX idx_linha ON bus_positions_filtered_materialize (linha);
# """

# with engine.connect() as conn:
# 	conn.execute(text(create_view_query))
# 	conn.execute(text(add_index_query))
#     # Commit the transaction
# 	conn.commit()

# create_table_query = f"""
# CREATE MATERIALIZED VIEW vw_buses_order AS
#     WITH filtered_data AS (
#         SELECT 
#             dense_rank() OVER (PARTITION BY ordem, linha ORDER BY datahoraservidor) AS time_ranking,
#             ordem,
#             linha,
#             geom,    
#             width_bucket(longitude, -43.726090, -42.951470, 1587) AS x,
#             width_bucket(latitude, -23.170790, -22.546410, 1389) AS y,
#             velocidade,
#             datahoraservidor
#         FROM 
#             public.bus_positions_filtered_materialize
#         WHERE longitude <= -42.951470
#             AND longitude >= -43.726090
#             AND latitude <= -22.546410
#             AND latitude >= -23.170790
#     ),
#     counts AS (
#         SELECT 
#             x, 
#             y, 
#             COUNT(*) AS cnt
#         FROM filtered_data
#         GROUP BY x, y
#     )
# SELECT 
#     fd.time_ranking,
#     fd.ordem,
#     fd.linha,
#     fd.geom,
#     fd.x,
#     fd.y,
#     fd.velocidade,
#     fd.datahoraservidor
# FROM 
#     filtered_data fd
# JOIN 
#     counts c ON fd.x = c.x AND fd.y = c.y
# WHERE 
#     c.cnt >= 3;

# """


# # Executar as consultas
# with engine.connect() as conn:
#     conn.execute(text(create_table_query))
#     conn.commit()

create_index_sql = """
CREATE INDEX idx_vw_buses_order_linha ON vw_buses_order(linha);

CREATE INDEX idx_vw_buses_order_ordem ON vw_buses_order(ordem);

CREATE INDEX idx_vw_buses_order_x_y ON vw_buses_order(x, y);

CREATE INDEX idx_vw_buses_order_ordem_linha ON vw_buses_order(ordem, linha);

CREATE INDEX idx_vw_buses_order_datahoraservidor ON vw_buses_order (datahoraservidor);

CREATE INDEX idx_vw_buses_order_time_ranking ON vw_buses_order(time_ranking);

"""

with engine.connect() as conn:
    conn.execute(text(create_index_sql))
    conn.commit()