import solara
import duckdb
import pandas as pd
import plotly.express as px 
import leafmap.maplibregl as leafmap 

def create_map():

    m = leafmap.Map(
        add_sidebar=True,
        add_floating_sidebar=False,
        sidebar_visible=True,
        layer_manager_expanded=False,
        height="800px",
    )
    m.add_basemap("Esri.WorldImagery", before_id=m.first_symbol_layer_id, visible=False)
    m.add_draw_control(controls=["polygon", "trash"])
    return m

    con = duckdb.connect()
    # 安裝和載入 httpfs (用於遠端檔案存取，如 S3)
    con.install_extension("httpfs")
    con.load_extension("httpfs")
    # 安裝和載入 spatial (用於空間資料處理)
    con.install_extension("spatial")
    con.load_extension("spatial")

@solara.component
def Page():
    m = create_map()
    return m.to_solara()