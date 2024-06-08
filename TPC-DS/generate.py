import pathlib
import os

import duckdb

SF = 100

cur_dir = os.path.dirname(os.path.abspath(__file__))

con = duckdb.connect()
con.sql('PRAGMA disable_progress_bar;SET preserve_insertion_order=false')
con.sql(f"CALL dsdgen(sf={SF})")
for tbl in ['call_center', 'catalog_returns', 'customer_address', 'customer_demographics', 'household_demographics', 'inventory', 'promotion', 'ship_mode', 'store_returns', 'time_dim', 'web_page', 'web_sales', 'catalog_page', 'catalog_sales', 'customer', 'date_dim', 'income_band', 'item', 'reason', 'store', 'store_sales', 'warehouse', 'web_returns', 'web_site']:
    table_dir = f'{cur_dir}/data/sf_{SF}/{tbl}'
    pathlib.Path(table_dir).mkdir(parents=True, exist_ok=True)
    con.sql(f"COPY (SELECT * FROM {tbl}) TO '{table_dir}/0.parquet'")
con.close()