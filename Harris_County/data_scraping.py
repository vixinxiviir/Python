from selenium import webdriver
from selenium.webdriver.support.ui import Select
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import time
import glob
import os

# DISCLAIMER: I don't currently know how to make new folders with Python, so I made all the folders for the files by
# hand. If you want to run this script, you'll need a folder
# inside your working director for each precinct (named after the corresponding p-value) with a folder for each year
# inside it.

# Note: this P value is the webpage source code value corresponding to which precinct we're looking at. It ranges from
# 305-380
p = 305
while p <= 380:
    year = 1993
    while year <= 2020:
        if isinstance(year / 4, int):  # Sets the from-date and to-date
            year = str(year)
            tdate1 = ['01/31/' + year, '02/29/' + year, '03/31/' + year, '04/30/' + year, '05/31/' + year,
                      '06/30/' + year, '07/31/' + year, '08/31/' + year, '09/30/' + year, '10/31/' + year,
                      '11/30/' + year, '12/31/' + year]
            fdate1 = ['01/01/' + year, '02/01/' + year, '03/01/' + year, '04/01/' + year, '05/01/' + year,
                      '06/01/' + year, '07/01/' + year, '08/01/' + year, '09/01/' + year, '10/01/' + year,
                      '11/01/' + year, '12/01/' + year]
        else:
            year = str(year)
            tdate1 = ['01/31/' + year, '02/28/' + year, '03/31/' + year, '04/30/' + year, '05/31/' + year,
                      '06/30/' + year, '07/31/' + year, '08/31/' + year, '09/30/' + year, '10/31/' + year,
                      '11/30/' + year, '12/31/' + year]
            fdate1 = ['01/01/' + year, '02/01/' + year, '03/01/' + year, '04/01/' + year, '05/01/' + year,
                      '06/01/' + year, '07/01/' + year, '08/01/' + year, '09/01/' + year, '10/01/' + year,
                      '11/01/' + year, '12/01/' + year]

        i = 0

        while i <= 11:
            p = str(p)
            chrome_options = webdriver.ChromeOptions()  # Initializes the driver
            prefs = {"download.default_directory": "C:/Users/Re(d)ginald/Documents/RA Stuff/harris county"}
            # prefs = {"download.default_direectory" : "Insert your filepath here"}
            chrome_options.add_experimental_option("prefs", prefs)
            driver = webdriver.Chrome(
                executable_path="C:/Users/Re(d)ginald/Documents/RA Stuff/harris county/chromedriver.exe")

            driver.get("https://jpwebsite.harriscountytx.gov/PublicExtracts/search.jsp")

            fdate = driver.find_element_by_id('fdate')
            fdate.send_keys(fdate1[i])
            tdate = driver.find_element_by_id('tdate')
            tdate.send_keys(Keys.CONTROL, 'a')
            tdate.send_keys(Keys.BACKSPACE)
            tdate.send_keys(tdate1[i])
            criminal = driver.find_element_by_css_selector("input[type='radio'][value='CR']").click()
            precinct = Select(driver.find_element_by_id('court'))
            precinct.select_by_value(p)
            casetype = Select(driver.find_element_by_id('casetype'))  # This is the sequence of clicks and strokes we
            # need to get the data we want

            driver.find_element_by_css_selector("input[type='button'][value='Request Your Data']").click()
            i = i + 1
            i = str(i)
            year = int(year)
            if isinstance(year / 4, int):
                year = str(year)
                date_dict = {"1": "C:/Users/Re(d)ginald/Downloads/CasesEntered-0101" + year + "To0131" + year + ".txt",
                             "2": "C:/Users/Re(d)ginald/Downloads/CasesEntered-0201" + year + "To0229" + year + ".txt",
                             "3": "C:/Users/Re(d)ginald/Downloads/CasesEntered-0301" + year + "To0331" + year + ".txt",
                             "4": "C:/Users/Re(d)ginald/Downloads/CasesEntered-0401" + year + "To0430" + year + ".txt",
                             "5": "C:/Users/Re(d)ginald/Downloads/CasesEntered-0501" + year + "To0531" + year + ".txt",
                             "6": "C:/Users/Re(d)ginald/Downloads/CasesEntered-0601" + year + "To0630" + year + ".txt",
                             "7": "C:/Users/Re(d)ginald/Downloads/CasesEntered-0701" + year + "To0731" + year + ".txt",
                             "8": "C:/Users/Re(d)ginald/Downloads/CasesEntered-0801" + year + "To0831" + year + ".txt",
                             "9": "C:/Users/Re(d)ginald/Downloads/CasesEntered-0901" + year + "To0930" + year + ".txt",
                             "10": "C:/Users/Re(d)ginald/Downloads/CasesEntered-1001" + year + "To1031" + year + ".txt",
                             "11": "C:/Users/Re(d)ginald/Downloads/CasesEntered-1101" + year + "To1130" + year + ".txt",
                             "12": "C:/Users/Re(d)ginald/Downloads/CasesEntered-1201" + year + "To1231" + year + ".txt"}

            else:
                year = str(year)
                date_dict = {"1": "C:/Users/Re(d)ginald/Downloads/CasesEntered-0101" + year + "To0131" + year + ".txt",
                             "2": "C:/Users/Re(d)ginald/Downloads/CasesEntered-0201" + year + "To0228" + year + ".txt",
                             "3": "C:/Users/Re(d)ginald/Downloads/CasesEntered-0301" + year + "To0331" + year + ".txt",
                             "4": "C:/Users/Re(d)ginald/Downloads/CasesEntered-0401" + year + "To0430" + year + ".txt",
                             "5": "C:/Users/Re(d)ginald/Downloads/CasesEntered-0501" + year + "To0531" + year + ".txt",
                             "6": "C:/Users/Re(d)ginald/Downloads/CasesEntered-0601" + year + "To0630" + year + ".txt",
                             "7": "C:/Users/Re(d)ginald/Downloads/CasesEntered-0701" + year + "To0731" + year + ".txt",
                             "8": "C:/Users/Re(d)ginald/Downloads/CasesEntered-0801" + year + "To0831" + year + ".txt",
                             "9": "C:/Users/Re(d)ginald/Downloads/CasesEntered-0901" + year + "To0930" + year + ".txt",
                             "10": "C:/Users/Re(d)ginald/Downloads/CasesEntered-1001" + year + "To1031" + year + ".txt",
                             "11": "C:/Users/Re(d)ginald/Downloads/CasesEntered-1101" + year + "To1130" + year + ".txt",
                             "12": "C:/Users/Re(d)ginald/Downloads/CasesEntered-1201" + year + "To1231" + year + ".txt"}

            while not os.path.exists(date_dict[i]):
                time.sleep(1)
            if os.path.isfile(date_dict[i]):
                os.rename(date_dict[i],
                          "C:/Users/Re(d)ginald/Documents/RA Stuff/harris county/input_data/" + p + "/" + year + "/month" + i)
                driver.quit()
            else:
                raise ValueError("That is not the correct file!")

            i = int(i)
            date_dict.clear()
        year = int(year)
        year = year + 1
    p = int(p)
    p = p + 5
