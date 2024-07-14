from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC 
from typing import Dict, Tuple
from airflow_comics.config import CONFIG
import logging


class WebDriverContextManager:
    def __init__(self):
        self.driver = None

    def __enter__(self):
        chrome_options = Options()
        chrome_options.add_argument("--headless")
        self.driver = webdriver.Chrome(options=chrome_options)
        return self.driver

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self.driver:
            self.driver.quit()

def scrape_comics_info(comics_history:Dict[str,dict]) -> Tuple[bool,Dict[str,dict]]:
    with WebDriverContextManager() as driver:
        anything_new = False
        all_comics_info = comics_history.copy()
        for comic in comics_history:
            driver.get(CONFIG['link'].replace('_id_', comic))
            WebDriverWait(driver, 10).until(EC.presence_of_element_located((By.PARTIAL_LINK_TEXT, '第')))
            links = driver.find_elements(By.PARTIAL_LINK_TEXT,'第')
            latest_chapter = [int(s) for s in links[-1].text.split() if s.isdigit()][0]
            all_comics_info[comic]['latest_chapter'] = latest_chapter
            if all_comics_info[comic]['previous_chapter'] < all_comics_info[comic]['latest_chapter']:
                all_comics_info[comic]['new_chapter'] = True
                anything_new = True
            else:
                all_comics_info[comic]['new_chapter'] = False

    return anything_new, all_comics_info
