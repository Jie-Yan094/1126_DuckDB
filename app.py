import solara
import duckdb
import pandas as pd
import plotly.express as px 
import leafmap.maplibregl as leafmap # <--- 新增引入 Leafmap

# -----------------
# 1. 狀態管理 (Reactive Variables)
# -----------------

all_countries = solara.reactive([])
selected_country = solara.reactive("United States")
# DataFrame 必須包含 'latitude' (緯度) 和 'longitude' (經度) 欄位
data = solara.reactive(pd.DataFrame()) 
CITIES_CSV_URL = 'https://data.gishub.org/duckdb/cities.csv'

# -----------------
# 2. 獲取國家清單 (use_effect, 保持不變)
# -----------------
@solara.use_effect(dependencies=[])
def load_country_list():
    """在組件初始化時，從 CSV 載入所有不重複的國家名稱。"""
    print("Loading country list...")
    try:
        con = duckdb.connect()
        # 查詢所有不重複的國家名稱並排序
        result = con.sql(f"""
            SELECT DISTINCT country 
            FROM '{CITIES_CSV_URL}'
            ORDER BY country;
        """).fetchall()
        
        country_list = [row[0] for row in result]
        
        all_countries.set(country_list)
        if country_list:
            selected_country.set(country_list[0]) 
        
        con.close()
    except Exception as e:
        print(f"Error loading countries: {e}")
        
# -----------------
# 3. 獲取篩選後的數據 (use_effect, 獲取經緯度)
# -----------------
@solara.use_effect(dependencies=[selected_country.value])
def load_filtered_data():
    """當 selected_country 改變時，執行新的 DuckDB 查詢，包含經緯度。"""
    country_name = selected_country.value
    if not country_name:
        return 

    print(f"Querying data for: {country_name}")
    try:
        con = duckdb.connect()
        
        # 查詢增加 latitude 和 longitude 欄位
        sql_query = f"""
        SELECT name, country, population, latitude, longitude
        FROM '{CITIES_CSV_URL}'
        WHERE country = '{country_name}'
        ORDER BY population DESC
        LIMIT 10;
        """
        
        df_result = con.sql(sql_query).df()
        
        data.set(df_result)
        
        con.close()
    except Exception as e:
        print(f"Error executing query: {e}")
        data.set(pd.DataFrame()) 


# -----------------
# 4. Leafmap 顯示組件
# -----------------

@solara.component
def CityMap(df: pd.DataFrame):
    """創建並顯示 Leafmap 地圖，標記城市點。"""
    
    if df.empty:
        return solara.Info("沒有城市數據可供地圖顯示。")

    # 1. 建立 Leafmap 實例
    # 使用第一個城市的經緯度作為中心點
    center = [df['latitude'].iloc[0], df['longitude'].iloc[0]]
    
    # 初始化地圖
    m = leafmap.Map(
        center=center, 
        zoom=4,                     # 調整初始縮放級別
        style="positron",           # 使用 Mapbox GL Style
        height="500px",             # 設定地圖高度
        add_sidebar=False
    )
    
    # 2. 將 DataFrame 轉換為 GeoJSON 並添加到地圖
    # 注意：我們使用 Leafmap 的內建方法來處理數據添加
    m.add_points_from_xy(
        df,
        x="longitude",
        y="latitude",
        tooltip="name", # 鼠標懸停時顯示城市名稱
        popup=["name", "population"], # 點擊時顯示名稱和人口
        color="red",
        size=8
    )

    # 3. 將 Leafmap 實例轉換為 Solara 組件
    return m.to_solara()


# -----------------
# 5. Solara 應用程式 Page 組件 (整合地圖)
# -----------------
@solara.component
def Page():
    # 標題
    solara.Title("DuckDB + Solara + Leafmap 城市地理分析")
    
    solara.Markdown("## 🌎 國家城市地理查詢與視覺化")
    
    with solara.Card(subtitle="篩選條件"):
        solara.Select(
            label="選擇國家",
            value=selected_country, 
            values=all_countries.value
        )
    
    # 顯示地圖、圖表和表格
    if selected_country.value and not data.value.empty:
        
        country_name = selected_country.value
        df = data.value
        
        # --- 顯示 Leafmap ---
        solara.Markdown("### 📍 城市地理位置分佈")
        CityMap(df) # <--- 使用新定義的 CityMap 組件
        
        # --- 顯示 Plotly 圖表 ---
        solara.Markdown(f"### 📊 {country_name} (前 {len(df)} 大城市人口分佈)")
        
        fig = px.bar(
            df, 
            x="name",               
            y="population",         
            color="population",     
            title=f"{country_name} 城市人口",
            labels={"name": "城市名稱", "population": "人口數"},
            height=400
        )
        fig.update_layout(xaxis_tickangle=-45)

        solara.FigurePlotly(fig)
        
        # --- 顯示數據表格 ---
        solara.Markdown(f"### 📋 數據表格")
        solara.DataFrame(df)

    elif selected_country.value:
        solara.Info(f"正在載入或 {selected_country.value} 沒有城市數據。")
    else:
        solara.Info("請等待國家清單載入...")