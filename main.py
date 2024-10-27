import requests
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys

options = webdriver.ChromeOptions()
options.add_experimental_option("detach", True)
options.add_experimental_option("excludeSwitches", ["enable-logging"])

url = "http://naver.com"

browser = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=options)
browser.get(url)

search = browser.find_element(By.ID, "query")
# input = input("매매가가 궁금한 아파트를 입력해주세요 : ")
input = "송파 헬리오시티"
search.send_keys(input+" 매물")
search.send_keys(Keys.RETURN)

url = browser.current_url
res = requests.get(url)
res.raise_for_status()

soup = BeautifulSoup(res.text, "lxml")

# with open("quiz.html", "w", encoding="utf8") as f:
#   f.write(soup.prettify())

data_rows = soup.find("table", attrs={"class": "list"}).find("tbody").find_all("tr", attrs={"class": "_land_tr_row"})

for idx, row in enumerate(data_rows):
  columns = row.find_all("td")
  print(f"====== 매물 {idx+1} ======")
  print("거래 :",columns[0].get_text())
  print("소재지 :",columns[1].get_text())
  print("단지명 :",columns[2].find("a").get_text())
  print("면적 :",columns[3].get_text(),"(공급/전용)")
  print("매물가 :",columns[4].get_text(),"(만원)")
  print("층 :", columns[5].get_text(), "(해당층/총층)")

browser.quit()