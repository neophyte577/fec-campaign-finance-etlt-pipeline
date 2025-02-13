from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

import numpy as np
import pandas as pd
from time import sleep

EXTENSION = '.csv'

CODEBOOKS = {
    'parties': 'party-code-descriptions',
    'committee_types': 'committee-type-code-descriptions',
    'report_types': 'report-type-code-descriptions',
    'transaction_types': 'transaction-type-code-descriptions'
}

def sleepytime():

    bounds = sorted(list(np.random.poisson(lam=0.577, size=2)))
    duration = np.random.uniform(low=bounds[0], high=bounds[1])
    return duration

def get_codebooks():

    chrome_options = Options()
    chrome_options.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/97.0.4692.71 Safari/537.36")
    chrome_options.add_argument("--start-minimized")
    chrome_options.add_argument("--disable-blink-features=AutomationControlled")
    chrome_options.add_experimental_option("excludeSwitches", ["enable-automation"])

    driver = webdriver.Chrome(options=chrome_options)

    code_dict = {}

    for codebook in CODEBOOKS:

        df = pd.DataFrame(columns=['code', 'description'])

        driver.get(f"https://www.fec.gov/campaign-finance-data/{CODEBOOKS[codebook]}")
        WebDriverWait(driver, 10).until(EC.visibility_of_element_located((By.CSS_SELECTOR, '.block-html table')))
        sleep(sleepytime())

        rows = driver.find_elements(By.TAG_NAME, 'tr')[1:]

        for index, row in enumerate(rows):
            row_elements = row.find_elements(By.TAG_NAME, 'td')
            issue_counter = 0
            try:
                code = row_elements[0].text.strip()
                description = row_elements[1].text.strip()
                df.loc[len(df)] = [code, description]
            except Exception as error:
                issue_counter += 1
                print(error)
                print(f'ISSUE: {issue_counter} in row {index} of {codebook}')
                print([element.get_attribute('outerHTML') for element in row_elements])

        code_dict[codebook] = df

    print(code_dict)

    driver.quit()

    return code_dict

def main():

    code_dict = get_codebooks()

    codebook_dir = './ingest/metadata/codebooks/'

    for codebook_name in code_dict:
        code_dict[codebook_name].to_csv(codebook_dir + codebook_name + EXTENSION, sep='|',index=False, )

if __name__ == '__main__':
    main()

