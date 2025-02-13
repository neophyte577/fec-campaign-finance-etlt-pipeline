from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

import numpy as np
import pandas as pd
from time import sleep

EXTENSION = '.csv'

DATASETS = {
    'candidate_summary': 'all-candidates-file-description',
    'candidate_master': 'candidate-master-file-description',
    'cand_comm_linkage': 'candidate-committee-linkage-file-description',
    'congressional_campaigns': 'current-campaigns-house-and-senate-file-description',
    'committee_master': 'committee-master-file-description',
    'pac_summary': 'pac-and-party-summary-file-description',
    'individual_contributions': 'contributions-individuals-file-description',
    'committee_contributions': 'contributions-committees-candidates-file-description',
    'committee_transactions': 'any-transaction-one-committee-another-file-description',
    'operating_expenditures': 'operating-expenditures-file-description'
}

def sleepytime():

    bounds = sorted(list(np.random.poisson(lam=0.577, size=2)))
    duration = np.random.uniform(low=bounds[0], high=bounds[1])
    return duration

def get_schemas():

    chrome_options = Options()
    chrome_options.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/97.0.4692.71 Safari/537.36")
    chrome_options.add_argument("--start-minimized")
    chrome_options.add_argument("--disable-blink-features=AutomationControlled")
    chrome_options.add_experimental_option("excludeSwitches", ["enable-automation"])

    driver = webdriver.Chrome(options=chrome_options)

    schema_dict = {}

    for dataset in DATASETS:

        df = pd.DataFrame(columns=['attribute', 'data_type'])

        driver.get(f"https://www.fec.gov/campaign-finance-data/{DATASETS[dataset]}")
        WebDriverWait(driver, 10).until(EC.visibility_of_element_located((By.CSS_SELECTOR, '.block-html table')))
        sleep(sleepytime())

        table = driver.execute_script("return document.querySelector('.block-html table').outerHTML;")

        rows = driver.find_elements(By.TAG_NAME, 'tr')[1:]

        for row in rows:
            row_elements = row.find_elements(By.TAG_NAME, 'td')
            attribute = row_elements[0].text.strip()
            data_type = row_elements[4].text.replace(' ', '').strip()
            if 'DATE' in data_type: # inconsistent date formats across tables 
                data_type = 'DATE'
            if ('(' in data_type) and (')' not in data_type): # FEC loves to omit closing parens in their tables for some reason
                data_type = data_type + ')'
            if 'or' in data_type: # FEC insists on specifying multiple VARCHAR with XOR rather than simply using the max of the two
                data_type = max(data_type.split('or'))
            if data_type == '': # at least one field was blank
                data_type = 'VARCHAR(420)'
            df.loc[len(df)] = [attribute, data_type]

        schema_dict[dataset] = df

    print(schema_dict)

    driver.quit()

    return schema_dict

def main():

    schema_dict = get_schemas()

    for dataset_name in schema_dict:
        schema_dict[dataset_name].to_csv(dataset_name + EXTENSION, index=False)

if __name__ == '__main__':
    main()

