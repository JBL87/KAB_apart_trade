from glob import glob
import pandas as pd
from selenium import webdriver
import helper
import time

from_folder = helper.download_folder

# 한국감정원 아파트거래현황
@helper.timer
def update_kab_apt_trade_volume(): # 한국감정원 아파트거래현황 다운로드
    codes = {'LHT_65040':'매입자거주지별_아파트거래',
            'LHT_65050':'거래주체별_아파트거래',
            'LHT_65060':'아파트거래현황_거래규모별',
            'LHT_65070':'거래원인별_아파트거래',
            'LHT_67040':'매입자거주지별_아파트매매거래',
            'LHT_67050':'거래주체별_아파트매매거래',
            'LHT_67060':'거래규모별_아파트매매거래',
            'LHT_67070':'매입자연령대별_아파트매매거래'}
    driver = webdriver.Chrome()
    for code in codes.keys():
        file_number = len(glob(from_folder + '*.xlsx'))
        url = f'https://www.r-one.co.kr/rone/resis/statistics/statisticsViewer.do?menuId={code}'
        driver.get(url)
        time.sleep(30)

        try: # 부동산원 통계 개편안내 문구가 뜸. 클릭해야지만 사라짐--
            driver.find_element_by_xpath("//body").click()
        except:
            pass
        driver.find_element_by_css_selector("#imgDownloadAll").click() # 전체다운로드 클릭
        time.sleep(10)

        while len(glob(from_folder + '*.xlsx')) == file_number:
            time.sleep(5)
        print(f'{codes[code]} 완료')
    driver.quit()
