import solara
import duckdb
import pandas as pd
import plotly.express as px 
import leafmap.maplibregl as leafmap 

CITIES_CSV_URL = 'https://data.gishub.org/duckdb/cities.csv'
all_countries = solara.reactive([])
selected_country = solara.reactive("") 
data_df = solara.reactive(pd.DataFrame()) 

@solara.use_effect(dependencies=[])
def load_country_list():
    """初始化：從 CSV 載入所有不重複的國家代碼。"""
    try:
        con = duckdb.connect()
        con.install_extension("httpfs")
        con.load_extension("httpfs")
        result = con.sql(f"""
            SELECT DISTINCT country 
            FROM '{CITIES_CSV_URL}'
            ORDER BY country;
        """).fetchall()
        
        country_list = [row[0] for row in result]
        all_countries.set(country_list)
        
        # 設定預設值
        if "USA" in country_list:
             selected_country.set("USA") 
        elif country_list:
             selected_country.set(country_list[0]) 
        
        con.close()
    except Exception as e:
        print(f"Error loading countries: {e}")

# B. 根據選中的國家篩選城市數據
@solara.use_effect(dependencies=[selected_country.value])
def load_filtered_data():
    """當 selected_country 變數改變時，重新執行 DuckDB 查詢。"""
    country_name = selected_country.value
    if not country_name:
        return 

    try:
        con = duckdb.connect()
        con.install_extension("httpfs")
        con.load_extension("httpfs")
        sql_query = f"""
        SELECT name, country, population, latitude, longitude
        FROM '{CITIES_CSV_URL}'
        WHERE country = '{country_name}'
        ORDER BY population DESC
        LIMIT 10;
        """
        
        df_result = con.sql(sql_query).df()
        data_df.set(df_result) # 更新響應式數據
        
        con.close()
    except Exception as e:
        print(f"Error executing query: {e}")
        data_df.set(pd.DataFrame())

# ----------------------------------------------------
# 3. 視覺化組件
# ----------------------------------------------------

@solara.component
def CityMap(df: pd.DataFrame):
    """(取代你的 create_map) 創建並顯示 Leafmap 地圖，標記城市點。"""
    
    if df.empty:
        return solara.Info("沒有城市數據可供地圖顯示。")

    # 使用數據的平均經緯度作為地圖中心
    center = [df['latitude'].mean(), df['longitude'].mean()]
    
    # 使用你的 Leafmap 參數設定
    m = leafmap.Map(
        center=center, 
        zoom=4,                     
        add_sidebar=True,
        add_floating_sidebar=False,
        sidebar_visible=True,
        layer_manager_expanded=False,
        height="800px", # 保持你設定的高度
    )
    
    # 添加底圖和繪圖工具 (你提供的功能)
    m.add_basemap("Esri.WorldImagery", before_id=m.first_symbol_layer_id, visible=False)
    m.add_draw_control(controls=["polygon", "trash"])

    # 添加城市點標記
    m.add_points_from_xy(
        df,
        x="longitude",
        y="latitude",
        tooltip="name", 
        popup=["name", "population"], 
        color="red",
        size=8
    )

    return m.to_solara()

@solara.component
def Page():
    
    solara.Title("城市地理人口分析 (DuckDB + Solara + Leafmap)")
    
    with solara.Card(title="城市數據篩選器"):
        # 綁定到 reactive 變數，當選單改變時，load_filtered_data 會自動運行
        solara.Select(
            label="選擇國家代碼",
            value=selected_country, 
            values=all_countries.value
        )
    
    # 僅當有數據時才繪製地圖
    if selected_country.value and not data_df.value.empty:
        
        country_code = selected_country.value
        df = data_df.value
        
        # 標題 (使用響應式變數)
        solara.Markdown("## Cities in " + country_code)
        
        # 顯示地圖
        CityMap(df) 
        
        # 顯示數據表格 (用於確認)
        solara.Markdown(f"### 📋 數據表格 (前 {len(df)} 大城市)")
        solara.DataFrame(df)

    elif selected_country.value:
         solara.Info(f"正在載入 {selected_country.value} 的數據...")
    else:
        solara.Info("正在載入國家清單...")