import re
import time
import pandas as pd
from selenium import webdriver
from selenium.common.exceptions import ElementClickInterceptedException
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support.ui import Select
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.common.by import By


class Crawler:

    def __init__(self):
        self.path = "C:/Users/codyr.DESKTOP-7O5CCFF/Documents/chromedriver.exe"
        self.options = webdriver.ChromeOptions()
        # self.options.add_argument("headless")
        self.driver = webdriver.Chrome(options=self.options)

    def crawl(self):
        check = 1
        driver = self.driver
        data = pd.DataFrame({"Year": [],
                                "Participant Name": [],
                                "Division Rank": [],
                                "Gender Rank": [],
                                "Overall Rank": [],
                                "Bib": [],
                                "Division": [],
                                "Points": [],
                                "Swim Time": [],
                                "Bike Time": [],
                                "Run Time": [],
                                "Overall Time": [],
                                "Swim Division Rank": [],
                                "Bike Division Rank": [],
                                "Run Division Rank": [],
                                "T1 Time": [],
                                "T2 Time": []
                                })

        driver.get("https://labs.competitor.com/result/subevent/61A15E83-504C-43B5-8FF6-029B92BA5F2A?filter=%7B%7D&order=ASC&page=1&perPage=50&sort=FinishRankOverall")

        wait = WebDriverWait(driver, 60)
        last_page = False
        # wait.until(EC.presence_of_element_located((By.ID, "onetrust-pc-btn-handler")))
        # driver.find_element(by=By.XPATH, value="//button[contains(text(), 'Accept All Cookies')]").click()
        while not last_page:
            wait.until(EC.presence_of_element_located((By.XPATH, "//div[@id='root']")))
            root = driver.find_element(by=By.XPATH, value="//div[@id='root']")
            buttons = root.find_elements(by=By.XPATH, value="//div[@aria-label='Expand']")
            source = root.get_attribute('innerHTML')
            # next_page = driver.find_element(by=By.XPATH, value="//button[@title='Next page']")
            time.sleep(3)
            wait.until(EC.presence_of_element_located((By.XPATH, ".//tr[@class='MuiTableRow-root MuiTableRow-hover RaDatagrid-row RaDatagrid-rowEven RaDatagrid-expandable RaDatagrid-selectable RaDatagrid-clickableRow css-11s7pgo']")))
            odd_results = root.find_elements(by=By.XPATH, value=".//tr[@class='MuiTableRow-root MuiTableRow-hover RaDatagrid-row RaDatagrid-rowOdd RaDatagrid-expandable RaDatagrid-selectable RaDatagrid-clickableRow css-11s7pgo']")
            for entry in odd_results:
                entry.click()
                time.sleep(1)
                extra_data_container = root.find_element(by=By.XPATH, value="//div[@class='resultContainer']")
                extra_data_text = extra_data_container.find_elements(by=By.XPATH, value="//div[@class='text']")
                ranks = root.find_elements(by=By.XPATH, value="//p[@class='rankNum']")
                data.loc[len(data.index)] = ""
                wait.until(EC.presence_of_element_located((By.TAG_NAME, "span")))
                overall_spans = entry.find_elements(by=By.TAG_NAME, value="span")
                driver.find_element(by=By.XPATH, value="//a[@href='#swimDetails']").click()
                time.sleep(.2)
                swim_table = driver.find_element(by=By.XPATH, value="//div[@id='swimDetails']")
                swim_data = swim_table.find_elements(by=By.XPATH, value=".//div[@class='text']")
                driver.find_element(by=By.XPATH, value="//a[@href='#bikeDetails']").click()
                time.sleep(.2)
                bike_table = driver.find_element(by=By.XPATH, value="//div[@id='bikeDetails']")
                bike_data = bike_table.find_elements(by=By.XPATH, value=".//div[@class='text']")
                driver.find_element(by=By.XPATH, value="//a[@href='#runDetails']").click()
                time.sleep(.2)
                run_table = driver.find_element(by=By.XPATH, value="//div[@id='runDetails']")
                run_data = run_table.find_elements(by=By.XPATH, value=".//div[@class='text']")
                transition_table = driver.find_element(by=By.XPATH, value="//div[@id='transitions']")
                transition_data = transition_table.find_elements(by=By.XPATH, value=".//div[@class='text']")
                data["Year"].iloc[len(data.index)-1] = 2023
                data["Participant Name"].iloc[len(data.index)-1] = overall_spans[1].text
                data["Overall Time"].iloc[len(data.index)-1] = overall_spans[2].text
                data["Overall Rank"].iloc[len(data.index)-1] = overall_spans[3].text
                data["Division Rank"].iloc[len(data.index)-1] = ranks[0].text
                data["Gender Rank"].iloc[len(data.index)-1] = ranks[1].text
                data["Bib"].iloc[len(data.index)-1] = extra_data_text[7].text
                data["Division"].iloc[len(data.index)-1] = extra_data_text[8].text
                data["Points"].iloc[len(data.index)-1] = extra_data_text[9].text
                data["Swim Time"].iloc[len(data.index)-1] = swim_data[4].text
                data["Swim Division Rank"].iloc[len(data.index)-1] = swim_data[5].text
                data["Bike Time"].iloc[len(data.index) - 1] = bike_data[4].text
                data["Bike Division Rank"].iloc[len(data.index) - 1] = bike_data[5].text
                data["Run Time"].iloc[len(data.index) - 1] = run_data[4].text
                data["Run Division Rank"].iloc[len(data.index) - 1] = run_data[5].text
                data["T1 Time"].iloc[len(data.index) - 1] = transition_data[2].text
                data["T2 Time"].iloc[len(data.index) - 1] = transition_data[3].text
                # if overall_spans[3].text == '954':
                #     driver.execute_script("window.scrollTo(0, 0)")
                entry.click()
                print(check)
                check += 1

            driver.execute_script("window.scrollTo(0, 0)")
            even_results = root.find_elements(by=By.XPATH,
                                             value=".//tr[@class='MuiTableRow-root MuiTableRow-hover RaDatagrid-row RaDatagrid-rowEven RaDatagrid-expandable RaDatagrid-selectable RaDatagrid-clickableRow css-11s7pgo']")
            for entry in even_results:
                entry.click()
                time.sleep(1)
                extra_data_container = root.find_element(by=By.XPATH, value="//div[@class='resultContainer']")
                extra_data_text = extra_data_container.find_elements(by=By.XPATH, value="//div[@class='text']")
                ranks = root.find_elements(by=By.XPATH, value="//p[@class='rankNum']")
                data.loc[len(data.index)] = ""
                wait.until(EC.presence_of_element_located((By.TAG_NAME, "span")))
                overall_spans = entry.find_elements(by=By.TAG_NAME, value="span")
                driver.find_element(by=By.XPATH, value="//a[@href='#swimDetails']").click()
                time.sleep(.2)
                swim_table = driver.find_element(by=By.XPATH, value="//div[@id='swimDetails']")
                swim_data = swim_table.find_elements(by=By.XPATH, value=".//div[@class='text']")
                driver.find_element(by=By.XPATH, value="//a[@href='#bikeDetails']").click()
                time.sleep(.2)
                bike_table = driver.find_element(by=By.XPATH, value="//div[@id='bikeDetails']")
                bike_data = bike_table.find_elements(by=By.XPATH, value=".//div[@class='text']")
                driver.find_element(by=By.XPATH, value="//a[@href='#runDetails']").click()
                time.sleep(.2)
                run_table = driver.find_element(by=By.XPATH, value="//div[@id='runDetails']")
                run_data = run_table.find_elements(by=By.XPATH, value=".//div[@class='text']")
                transition_table = driver.find_element(by=By.XPATH, value="//div[@id='transitions']")
                transition_data = transition_table.find_elements(by=By.XPATH, value=".//div[@class='text']")
                data["Year"].iloc[len(data.index) - 1] = 2023
                data["Participant Name"].iloc[len(data.index) - 1] = overall_spans[1].text
                data["Overall Time"].iloc[len(data.index) - 1] = overall_spans[2].text
                data["Overall Rank"].iloc[len(data.index) - 1] = overall_spans[3].text
                data["Division Rank"].iloc[len(data.index) - 1] = ranks[0].text
                data["Gender Rank"].iloc[len(data.index) - 1] = ranks[1].text
                data["Bib"].iloc[len(data.index) - 1] = extra_data_text[7].text
                data["Division"].iloc[len(data.index) - 1] = extra_data_text[8].text
                data["Points"].iloc[len(data.index) - 1] = extra_data_text[9].text
                data["Swim Time"].iloc[len(data.index) - 1] = swim_data[4].text
                data["Swim Division Rank"].iloc[len(data.index) - 1] = swim_data[5].text
                data["Bike Time"].iloc[len(data.index) - 1] = bike_data[4].text
                data["Bike Division Rank"].iloc[len(data.index) - 1] = bike_data[5].text
                data["Run Time"].iloc[len(data.index) - 1] = run_data[4].text
                data["Run Division Rank"].iloc[len(data.index) - 1] = run_data[5].text
                data["T1 Time"].iloc[len(data.index) - 1] = transition_data[2].text
                data["T2 Time"].iloc[len(data.index) - 1] = transition_data[3].text
                if overall_spans[3].text == '1995':
                    driver.execute_script("window.scrollTo(0, document.body.scrollHeight)")
                entry.click()
                print(check)
                check += 1
            driver.execute_script("window.scrollTo(0, document.body.scrollHeight)")
            wait.until(EC.presence_of_element_located((By.XPATH, "//button[@aria-label='Go to next page']")))
            try:
                data.to_csv("C:/Users/codyr.DESKTOP-7O5CCFF/Documents/IronManWomens2023.csv")
                driver.find_element(by=By.XPATH, value="//button[@aria-label='Go to next page']").click()
            except ElementClickInterceptedException:
                last_page = True
                print("Success!")
                data.to_csv("C:/Users/codyr.DESKTOP-7O5CCFF/Documents/IronManWomens2023.csv")



crawler = Crawler()
crawler.crawl()
print("Waiting...")