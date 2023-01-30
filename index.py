import pandas as pd
import psycopg2

conn = psycopg2.connect(database="db_name", user="db_user",password="db_password",host="localhost",port="5432")
cur = conn.cursor()
cur.execute("""CREATE TABLE IF NOT EXISTS foreo (Store_No text, 
                                 Store text, 
                                 TY_Units float,
                                 LY_Units float,
                                 TW_Sales float,
                                 LW_Sales float,
                                 LW_Var float,
                                 LY_Sales float,
                                 LY_Var float,
                                 YTD_Sales float,
                                 LYTD_Sales float,
                                 LYTD_Var float);""")
conn.commit()
conn.close()
cur.close()


class Data:
  def __init__(self, store_number, store, ty_units, ly_units, tw_sales, lw_sales, lw_var, ly_sales, ly_var, ytd_sales, lytd_sales, lytd_var):
    self.store_number = store_number
    self.store = store
    self.ty_units = ty_units
    self.ly_units = ly_units
    self.tw_sales = tw_sales
    self.lw_sales = lw_sales
    self.lw_var = lw_var
    self.ly_sales = ly_sales
    self.ly_var = ly_var
    self.ytd_sales = ytd_sales
    self.lytd_sales = lytd_sales
    self.lytd_var = lytd_var



def execute(path, sheet_name):
  first_sheet = pd.read_excel(path, sheet_name, index_col=1, header= 5)

  raw_data = first_sheet.to_numpy()
  for raw in raw_data:
    data = Data(raw[1], raw[2], raw[4], raw[5], raw[6], raw[7], raw[8], raw[9], raw[10], raw[11], raw[12], raw[13])

    if raw[1] == 'Total':
        continue
   
    conn = psycopg2.connect(database="db_name", user="db_user",password="db_password",host="localhost",port="5432")
    cur = conn.cursor()
    cur.execute("""INSERT INTO foreo VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)""",(data.store_number,data.store,data.ty_units,data.ly_units,data.tw_sales,data.lw_sales,data.lw_var,data.ly_sales,data.ly_var,data.ytd_sales,data.lytd_sales,data.lytd_var))
    conn.commit()
    cur.close()
    conn.close()
  

    
execute('SpaceNK_2.0.xlsx', 'Last Week Report by Store')
