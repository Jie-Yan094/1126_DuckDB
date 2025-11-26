import solara
import duckdb
import pandas as pd
import plotly.express as px # <--- 新增引入 Plotly Express

# -----------------
# 1. 狀態管理 (Reactive Variables)
# ... (保持不變) ...
# -----------------
all_countries = solara.reactive([])
selected_country = solara.reactive("United States")
data = solara.reactive(pd.DataFrame())
CITIES_CSV_URL = 'https://data.gishub.org/duckdb/cities.csv'


# -----------------
# 2. 獲取國家清單 (use_effect, 保持不變)
# ...
# -----------------
@solara.use_effect(dependencies=[])
def load_country_list():
    """在組件初始化時，從 CSV 載入所有不重複的國家名稱。"""
    print("Loading country list...")
    try:
        con = duckdb.connect()
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
# 3. 獲取篩選後的數據 (use_effect, 保持不變)
# ...
# -----------------
@solara.use_effect(dependencies=[selected_country.value])
def load_filtered_data():
    """當 selected_country 改變時，執行新的 DuckDB 查詢。"""
    country_name = selected_country.value
    if not country_name:
        return 

    print(f"Querying data for: {country_name}")
    try:
        con = duckdb.connect()
        
        # 查詢前 10 大城市
        sql_query = f"""
        SELECT name, country, population 
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
# 4. Solara 應用程式組件
# -----------------
@solara.component
def Page():
    # 標題
    solara.Title("DuckDB + Solara 城市人口分析")
    
    solara.Markdown("## 🌐 國家城市人口查詢與視覺化")
    
    with solara.Card(subtitle="篩選條件"):
        # 下拉選單組件
        solara.Select(
            label="選擇國家",
            value=selected_country, 
            values=all_countries.value
        )
    
    # 顯示查詢結果和圖表
    if selected_country.value and not data.value.empty:
        
        country_name = selected_country.value
        df = data.value
        
        solara.Markdown(f"### 📊 {country_name} (前 {len(df)} 大城市人口分佈)")

        # --- 繪製 Plotly 圖表 ---
        # 1. 使用 Plotly Express 建立條形圖
        fig = px.bar(
            df, 
            x="name",               # X 軸：城市名稱
            y="population",         # Y 軸：人口數
            color="population",     # 顏色深淺也根據人口數
            title=f"{country_name} 城市人口",
            labels={"name": "城市名稱", "population": "人口數"},
            height=400
        )
        # 調整圖表排版，讓城市名稱更易讀
        fig.update_layout(xaxis_tickangle=-45)

        # 2. 使用 solara.FigurePlotly 顯示圖表
        solara.FigurePlotly(fig)
        
        # --- 顯示數據表格 ---
        solara.Markdown(f"### 📋 數據表格")
        solara.DataFrame(df)

    elif selected_country.value:
        solara.Info(f"正在載入或 {selected_country.value} 沒有城市數據。")
    else:
        solara.Info("請等待國家清單載入...")
