#!/usr/bin/env python
# coding: utf-8

# In[1]:


from selenium import webdriver
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import Select
from webdriver_manager.chrome import ChromeDriverManager
import time
import requests
import pyodbc
import logging
import datetime
from bs4 import BeautifulSoup


# In[2]:


server = '' 
database = ''
username = '' 
password = '' 
conn = pyodbc.connect('Driver={ODBC Driver 17 for SQL Server};'
                          'Server=' + server + ';'
                          'Database=' + database + ';'
                          'UID=' + username + ';'
                          'PWD=' + password + ';'
                          'Trusted_Connection=no;')
cursor = conn.cursor()


# In[3]:


server = '10.2.11.32' 
database = 'DB_SMS'
username = 'guid' 
password = 'gpwd' 
conn_sp = pyodbc.connect('Driver={ODBC Driver 17 for SQL Server};'
                          'Server=' + server + ';'
                          'Database=' + database + ';'
                          'UID=' + username + ';'
                          'PWD=' + password + ';'
                          'Trusted_Connection=no;')
cursor_sp = conn_sp.cursor()


# In[4]:


cursor.execute("{CALL [dbo].[GetOpdList]}")

# 获取存储过程的结果集
rows = cursor.fetchall()

# 提取结果中的 chMRNo
IDs = [row.chMRNo for row in rows]

# 关闭数据库连接
#conn_sp.close()

# 使用 IDs 进行后续操作
#print(IDs)


# In[5]:


print("此次抓取數量",len(IDs))


# In[6]:


url = 'https://10.241.219.45/CMSUserWeb'
driver = webdriver.Chrome()
driver.get(url)
#driver.maximize_window()
time.sleep(1)
driver.find_element('xpath','//*[@id="details-button"]').click()
driver.find_element('xpath','//*[@id="proceed-link"]').click()
locator = (By.CLASS_NAME, "mat-form-field-flex")  # 定位器
search_input = WebDriverWait(driver, 20).until(
    EC.presence_of_element_located(locator),
    "找不到指定的元素"
)
print("----------網頁成功開啟----------")
Hospital_ID="1140030012"
PassWord="413034"
print("--------開始輸入基本資料--------")
driver.find_element('id','mat-input-0').send_keys(Hospital_ID)#輸入醫事機構代碼
print("===>醫事機構代碼:",Hospital_ID,"輸入完成")
#time.sleep(0.5)
driver.find_element('id','mat-input-1').send_keys(PassWord)#輸入卡片密碼
print("===>卡片密碼:",PassWord,"輸入完成")
#time.sleep(0.5)
max_retry = 3  # 最大重試次數
retry_count = 0
while retry_count < max_retry:
    try:
        driver.find_element('xpath', '/html/body/app-root/app-login/div/div/mat-card/mat-card-content/form/button').click()
        locator = (By.CLASS_NAME, "breadcrumb__label")  # 定位器
        search_input = WebDriverWait(driver, 10).until(
            EC.presence_of_element_located(locator),
            "登錄失敗"
        )
        break  # 登錄成功，跳出迴圈
    except Exception as e:
        print("登錄失敗，進行重試:", e)
        retry_count += 1
        if retry_count >= max_retry:
            print("已達到最大重試次數")
            break

print("登錄成功")
driver.find_element('xpath','/html/body/app-root/app-logined/mat-sidenav-container/mat-sidenav/div/mat-nav-list/div/a[2]/div/span').click()
locator = (By.ID, "IDNum") #定位器
search_input = WebDriverWait(driver, 20).until(
    EC.presence_of_element_located(locator),
    "加載失敗"
)
driver.find_element('xpath','//*[@id="mat-select-1"]/div/div[1]').click()
time.sleep(2)
check=driver.find_element('id','mat-option-1').text


if check == "庫賈CJD":
    print("庫賈CJD條件確認完成")
    driver.find_element('id', 'mat-option-1').click()  #庫賈CJD
    #計數器
    updated_count = 0
    inserted_count = 0
    invalid_count = 0
    CJD_count=0
    for ID in IDs:        
        count= 0
        while True:
            driver.find_element('id', 'IDNum').send_keys(ID)    
            driver.find_element('id', 'userPageSubmitBtn').click()  # 查詢按鈕

            time.sleep(0.1)
            search = driver.find_element('xpath', '/html/body/app-root/app-logined/mat-sidenav-container/mat-sidenav-content/div/app-articulation-search/mat-card/div/div[4]').text
            chDate = datetime.date.today()
            chID = search[7:17].replace(" ", "")
            chResult = search[17:].replace(" ", "")
            if chResult == "不在庫賈CJD疾病別的管制名單中":
                chResult = "非庫賈CJD疾病別的管制名單中"
            else:
                chResult = "庫賈CJD管制名單"
            driver.find_element('id', 'IDNum').clear()        
            if len(chID) != 10:
                print(chID, "ID長度不足10碼，跳過該筆資料")
                invalid_count += 1
                break
            if chID == ID:
                try:
                    #print(chID,"=",ID)
                    cursor.execute("INSERT INTO BSE_search (chID, chDate, chResult) VALUES (?, ?, ?)", (chID, chDate, chResult))
                    conn.commit()
                    inserted_count += 1
                    print(ID, "資料已新增")
                except pyodbc.IntegrityError:
                    # 插入失敗，主鍵重複，執行 UPDATE
                    cursor.execute("UPDATE BSE_search SET chDate=?, chResult=? WHERE chID=?", (chDate, chResult, chID))
                    conn.commit()
                    updated_count += 1
                    print(ID, "資料已更新")
                break
            else:
                count += 1
                if count >= 20:
                    print(ID, "此資料無法新增")
                    break
                else:
                    time.sleep(0.1)
    today = datetime.date.today()
    try:
        # 執行 SQL 查詢
        cursor.execute("SELECT chID FROM BSE_search WHERE chResult =? AND chDate=?", ("庫賈CJD管制名單", today))
        #conn.commit()
        rows = cursor.fetchall()
        ID_CJD = [row[0] for row in rows]
        print("ID_CJD List:", ID_CJD)

        # 如果沒有ID_CJD，跳出try塊
        if not ID_CJD:
            raise Exception("無新增庫賈CJD患者")

        cursor.execute("DELETE  FROM BSE_search WHERE chResult =? AND chDate=?", ("庫賈CJD管制名單", today))
        conn.commit()
        cursor.execute("{CALL [dbo].[GetOpdList]}")
        rows = cursor.fetchall()
        for id_cjd in ID_CJD:
            for row in rows:
                if row.chMRNo == id_cjd:
                    chNote = str(row.chNote)
                    chMobile = str(row.chMobile)
                    print("chNote:", chNote)
                    print("chMobile:", chMobile)
                    cursor_sp.execute("{CALL [dbo].[SendSmsCJD] (@mobile = ?, @msg = ?)}", (chMobile, chNote))
                    conn_sp.commit()
                    cursor.execute("INSERT INTO BSE_search (chID, chDate, chResult) VALUES (?, ?, ?)", (id_cjd, chDate, "庫賈CJD管制名單"))
                    conn.commit()
                    CJD_count += 1
                    break
    except Exception as e:
        print(e)
else:
    print("請重新檢查網頁原始碼")
    
# 關閉資料庫連線
conn.close()
print("新增的資料筆數：", inserted_count)
print("傳送CJD病患簡訊數",CJD_count)
print("更新的資料筆數：", updated_count)
print("ID有誤的筆數：", invalid_count)


# In[7]:


# 設定log檔案名稱
log_file = "狂牛爬蟲紀錄.txt"
# 設定log輸出格式
log_format = "%(asctime)s - %(levelname)s - %(message)s"
logging.basicConfig(filename=log_file, level=logging.INFO, format=log_format)
#logging.info("更新的資料筆數：%d", updated_count)
#logging.info("新增的資料筆數：%d", inserted_count)
logging.info("傳送CJD病患簡訊數：%d", CJD_count)
#logging.info("ID長度不足10碼的資料筆數：%d", invalid_count)
logging.shutdown()


# In[8]:


conn_sp.close()


# In[ ]:




