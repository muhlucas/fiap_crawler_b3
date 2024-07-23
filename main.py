import io
import os
import boto3
from datetime import datetime
import pandas as pd
from dotenv import load_dotenv
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import Select, WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from botocore.exceptions import NoCredentialsError, PartialCredentialsError


B3_URL = "https://sistemaswebb3-listados.b3.com.br/indexPage/day/IBOV?language=pt-br"
SLEEP_SECONDS = 5


def get_data_from_table(webpage):
    today_date = datetime.now().strftime("%Y-%m-%d")
    table = webpage.find_element(By.TAG_NAME, "table")
    headers = [header.text for header in table.find_elements(By.TAG_NAME, "th")]
    headers.append("Date")

    if len(headers) >= 3:
        second_sub_th = headers.pop()
        first_sub_th = headers.pop()
        parent_th = headers.pop()
        headers.append(f"{parent_th} - {first_sub_th}")
        headers.append(f"{parent_th} - {second_sub_th}")

    rows = table.find_elements(By.TAG_NAME, "tr")
    table_data = []
    for row in rows[1:len(rows) - 1]:
        cells = row.find_elements(By.TAG_NAME, "td")
        if len(cells) > 0:
            row_data = {headers[i]: cells[i].text for i in range(len(cells))}
            row_data["Date"] = today_date
            table_data.append(row_data)

    for row in rows[len(rows) - 1:]:
        cells = row.find_elements(By.TAG_NAME, "td")
        row_data = {headers[0]: cells[0].text, headers[len(headers) - 3]: cells[1].text}
        if not cells[2].text == '':
            row_data[headers[len(headers) - 2]] = cells[2].text
            row_data[headers[len(headers) - 1]] = cells[3].text
        row_data["Date"] = today_date
        table_data.append(row_data)

    return table_data


def next_page(webpage):
    pagination = webpage.find_element(By.CLASS_NAME, 'pagination-next')
    if 'disabled' in pagination.get_attribute('class'):
        return False
    pagination.click()
    return True


def get_all_table(webpage):
    full_table = []
    while True:
        data = get_data_from_table(webpage)
        full_table.extend(data)
        has_next = next_page(webpage)
        if not has_next:
            break
    return full_table


def utf8_encoding(df):
    for col in df.select_dtypes(include=['object']).columns:
        df[col] = df[col].apply(lambda x: x.encode('utf-8').decode('utf-8') if isinstance(x, str) else x)
    return df


def table_to_parquet(table):
    df = pd.DataFrame(table)
    df = utf8_encoding(df)
    buffer = io.BytesIO()
    df.to_parquet(buffer, index=False, engine="pyarrow")
    buffer.seek(0)
    return buffer


def upload_to_s3(buffer, file_name):
    try:
        bucket_name = os.environ['AWS_S3_BUCKET_NAME']
        aws_access_key_id = os.environ['AWS_ACCESS_KEY']
        aws_secret_access_key = os.environ['AWS_ACCESS_SECRET']
        region_name = os.environ['AWS_REGION_NAME']

        s3 = boto3.client(
            's3',
            aws_access_key_id=aws_access_key_id,
            aws_secret_access_key=aws_secret_access_key,
            region_name=region_name
        )
        s3.upload_fileobj(buffer, bucket_name, file_name)
        print(f"Successfully uploaded {bucket_name}/{file_name}")
    except (NoCredentialsError, PartialCredentialsError) as e:
        print("Credentials not available", e)


def main():
    load_dotenv()

    options = webdriver.ChromeOptions()
    options.add_argument('--headless')
    options.add_argument('--disable-gpu')
    options.add_argument('--no-sandbox')
    options.add_argument('--disable-dev-shm-usage')

    with webdriver.Chrome(options=options) as driver:
        driver.get(B3_URL)

        WebDriverWait(driver, SLEEP_SECONDS).until(
            EC.presence_of_element_located((By.ID, "segment"))
        )

        segment_element = driver.find_element(By.ID, "segment")
        select = Select(segment_element)
        # VALOR 2 SIGNIFICA SETOR DE ATUAÇÃO
        select.select_by_value('2')

        WebDriverWait(driver, SLEEP_SECONDS).until(
            EC.presence_of_element_located((By.TAG_NAME, "table"))
        )

        table = get_all_table(driver)
        parquet_buffer = table_to_parquet(table)
        upload_to_s3(parquet_buffer, 'b3.parquet')

        driver.quit()


if __name__ == '__main__':
    main()
