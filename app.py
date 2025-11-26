import solara
import duckdb
import pandas as pd
import plotly.express as px 
import leafmap.maplibregl as leafmap 

# 檔案路徑 (使用遠端 URL)
CITIES_CSV_URL = 'https://data.gishub.org/duckdb/cities.csv'

# -----------------
# 1. 狀態管理 (Reactive Variables)
# -----------------
# 用於儲存所有國家/地區的清單
all_countries = solara.reactive([])
# 儲存使用者在下拉選單中選擇的國家/地區代碼
selected_country = solara.reactive("") 
# 儲存篩選後的城市數據
data_df = solara.reactive(pd.DataFrame()) 

# ----------------------------------------------------
# 2. 數據獲取邏輯 (請用此區塊覆蓋您檔案中對應的內容)
# ----------------------------------------------------

# A. 載入所有國家清單 (只在應用程式啟動時執行一次)
@solara.use_effect(dependencies=[])
def load_country_list():
    """初始化：從 CSV 載入所有不重複的國家代碼。"""
    print("Loading country list...")
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

    print(f"Querying data for: {country_name}")
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
        data_df.set(df_result) 
        
        con.close()
    except Exception as e:
        print(f"Error executing query: {e}")
        data_df.set(pd.DataFrame())
        
# ----------------------------------------------------
# 3. 視覺化組件
# ----------------------------------------------------

@solara.component
def CityMap(df: pd.DataFrame):
    """創建並顯示 Leafmap 地圖，標記城市點 (使用您提供的設定)。"""
    
    if df.empty:
        return solara.Info("沒有城市數據可供地圖顯示。")

    # 使用數據的平均經緯度作為地圖中心
    center = [df['latitude'].mean(), df['longitude'].mean()]
    
    m = leafmap.Map(
        center=center, 
        zoom=4,                     
        # 您指定的 Leafmap 參數
        add_sidebar=True,
        add_floating_sidebar=False,
        sidebar_visible=True,
        layer_manager_expanded=False,
        height="800px", 
    )
    
    # 添加底圖和繪圖工具
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

# ----------------------------------------------------
# 4. 頁面佈局組件
# ----------------------------------------------------

@solara.component
def Page():
    
    solara.Title("城市地理人口分析 (DuckDB + Solara + Leafmap)")
    
    with solara.Card(title="城市數據篩選器"):
        # 綁定到 reactive 變數
        solara.Select(
            label="選擇國家代碼",
            value=selected_country, 
            values=all_countries.value
        )
    
    # 僅當有數據時才繪製地圖和圖表
    if selected_country.value and not data_df.value.empty:
        
        country_code = selected_country.value
        df = data_df.value
        
        # 標題
        solara.Markdown("## Cities in " + country_code)
        
        # 顯示地圖
        CityMap(df) 
        
        # 顯示數據表格 (用於確認)
        solara.Markdown(f"### 📋 數據表格 (前 {len(df)} 大城市)")
        solara.DataFrame(df)
        
        # 額外添加 Plotly 圖表，使應用程式更完整
        solara.Markdown(f"### 📊 {country_code} 人口分佈 (Plotly)")
        fig = px.bar(
            df, 
            x="name",               
            y="population",         
            color="population",     
            title=f"{country_code} 城市人口",
            labels={"name": "城市名稱", "population": "人口數"},
            height=400 
        )
        fig.update_layout(xaxis_tickangle=-45)
        solara.FigurePlotly(fig)


    elif selected_country.value:
         solara.Info(f"正在載入 {selected_country.value} 的數據...")
    else:
        solara.Info("正在載入國家清單...")