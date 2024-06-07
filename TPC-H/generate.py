import pathlib
import os

import duckdb

SF = 1

cur_dir = os.path.dirname(os.path.abspath(__file__))

con = duckdb.connect()
con.sql('PRAGMA disable_progress_bar;SET preserve_insertion_order=false')
for i in range(SF):
  con.sql(f"CALL dbgen(sf={SF}, children={SF}, step={i})")
  for tbl in ['nation','region','customer','supplier','lineitem','orders','partsupp','part']:
     table_dir = f'{cur_dir}/data/sf_{SF}/{tbl}'
     pathlib.Path(table_dir).mkdir(parents=True, exist_ok=True)
     con.sql(f"COPY (SELECT * FROM {tbl}) TO '{table_dir}/{i}.parquet'")
con.close()