import io
import json
import pandas as pd
import requests
import itertools
from datetime import datetime

from time import sleep

from airflow.models import DAG

from airflow.hooks.base_hook import BaseHook
from airflow.hooks.S3_hook import S3Hook
from airflow.operators.python import PythonOperator
from airflow.providers.google.suite.hooks.sheets import GSheetsHook


global_kwargs = {
    'bucket_name': 'chevsky-airflow-data',
    'date_from': '2024-01-01',
    'date_to': datetime.now().strftime("%Y-%m-%d"),
    'spreadsheet_id':'1cKFRqFvlWPSy-OKGDJKQ2izXlgFd9_pHgcszvhuCExk',
    'range': 'data!A1',
    'range_all': 'data!A1:Z'
}


def map_colnames(colnames):
    col_name_mapping = {
        'dt': 'дата', 
        'month': 'месяц',
        'week': 'неделя',
        'nm_id': 'артикул', 
        'item_name': 'товар',  
        'campaign_name': 'название рк', 
        'campaign_ids': 'номера рк', 
        'views': 'показы', 
        'clicks': 'клики', 
        'ad_spend': 'расходы', 
        'orders': 'заказы с рекламы', 
        'items_sold': 'продажи рк, шт.',
        'sum_price': 'продажи рк, руб.',
        'openCardCount': 'переход в карточку', 
        'addToCartCount': 'добавить в корзину', 
        'ordersCount': 'продажи всего, шт.', 
        'ordersSumRub': 'продажи всего, руб', 
        'buyoutsCount': 'выкуплено всего, шт.', 
        'buyoutsSumRub': 'выкуплено всего, руб.',
    }
    return [col_name_mapping.get(cn, cn) for cn in colnames]


def batchify(items, n):
    for i in range(0, len(items), n):
        yield items[i:i + n]


def get_df_from_s3(conn_id, bucket_name, key):
    hook = S3Hook(conn_id)
    file_content = hook.read_key(key=key, bucket_name=bucket_name)
    return pd.read_csv(io.StringIO(file_content))

    # Load the content into a pandas DataFrame
    df = pd.read_csv(io.StringIO(file_content))

def get_stock(**kwargs):
    conn_id = "wb_statistics"
    conn = BaseHook.get_connection(conn_id)
    extras = conn.extra_dejson
    api_token = extras.get("Authorization")

    # Extract arguments from kwargs
    date_from = kwargs.get('date_from')
    date_to = kwargs.get('date_to')
    bucket_name = kwargs.get('bucket_name')

    hook = S3Hook('aws')

    url = 'https://statistics-api.wildberries.ru/api/v1/supplier/stocks'

    headers = {
        'Authorization': api_token
    }

    payload = {
        'dateFrom': date_from
    }

    task_completed = False
    while not task_completed:
        res = requests.get(url, headers=headers, params=payload)
        try:
            res.raise_for_status()
            task_completed = True
        except Exception as e:
            print('ERROR', e)
            sleep(20)
    
    # df = pd.DataFrame([{'foo': 'bar'}])
    df = pd.DataFrame(res.json())
    nm_ids = [int(x) for x in df['nmId'].unique()]

    # Format datetime now to a safe string for filenames
    now_str = datetime.now().strftime("%Y%m%d_%H%M%S")
    file_name = f'stock_{now_str}.csv'
    csv_buffer = io.StringIO()
    df.to_csv(csv_buffer, index=False)

    # Reset pointer to beginning of StringIO buffer before reading
    csv_buffer.seek(0)

    hook.load_string(
        string_data=csv_buffer.getvalue(),
        bucket_name=bucket_name,
        key=file_name,
        replace=True
    )

    # Use XCom to push the bucket name and file name
    ti = kwargs['ti']
    ti.xcom_push(key='bucket_name', value=bucket_name)
    ti.xcom_push(key='file_name', value=file_name)
    ti.xcom_push(key='nm_ids', value=nm_ids)


def get_funnel(**kwargs):
    conn_id = "wb_statistics"
    conn = BaseHook.get_connection(conn_id)
    extras = conn.extra_dejson
    api_token = extras.get("Authorization")

    # Extract arguments from kwargs
    ti = kwargs['ti']
    nm_ids = ti.xcom_pull(key='nm_ids')

    date_from = kwargs.get('date_from')
    date_to =   kwargs.get('date_to')
    bucket_name = kwargs.get('bucket_name')

    
    print(date_from, date_to)

    hook = S3Hook('aws')

    url = 'https://suppliers-api.wildberries.ru/content/v1/analytics/nm-report/detail/history'

    headers = {
        'Authorization': api_token
    }

    data = []
    batches = list(batchify(nm_ids, 20))
    n_batches = len(list(batches))

    print(nm_ids)
    print()
    print(n_batches)


    for batch in batches:
        task_completed = False
        payload = {
            'period': {
                'begin': date_from,
                'end': date_to,
            },
            'nmIDs': batch,
            'aggregationLevel': 'day'
        }
        while not task_completed:
            res = requests.post(url, headers=headers, json=payload)
            try:
                res.raise_for_status()
                task_completed = True
            except Exception as e:
                print('ERROR', e)
                sleep(20)
        data.append(res.json())
        print(f'Done: {len(data)}/{n_batches}')

    records = []

    for batch in [x['data'] for x in data]:
        for item in batch:
            for dt in item['history']:
                dt['nm_id'] = item['nmID']
                dt['item_name'] = item['imtName']
                dt['vendor_code'] = item['vendorCode']
                records.append(dt)

    
    df = pd.DataFrame(records)

    # Format datetime now to a safe string for filenames
    now_str = datetime.now().strftime("%Y%m%d_%H%M%S")
    file_name = f'funnel_{now_str}.csv'
    csv_buffer = io.StringIO()
    df.to_csv(csv_buffer, index=False)

    # Reset pointer to beginning of StringIO buffer before reading
    csv_buffer.seek(0)

    hook.load_string(
        string_data=csv_buffer.getvalue(),
        bucket_name=bucket_name,
        key=file_name,
        replace=True
    )

    # Use XCom to push the bucket name and file name
    ti.xcom_push(key='bucket_name', value=bucket_name)
    ti.xcom_push(key='file_name', value=file_name)


def get_campaigns(**kwargs):
    conn_id = "wb_statistics"
    conn = BaseHook.get_connection(conn_id)
    extras = conn.extra_dejson
    api_token = extras.get("Authorization")

    headers = {
        'Authorization': api_token
    }

    camps = []

    statuses = {
        -1: 'кампания в процессе удаления',
        4: 'готова к запуску',
        7: 'кампания завершена',
        8: 'отказался',
        9: 'идут показы',
        11: 'кампания на паузе'
    }

    for status_id in [9]: # statuses.keys():
        params = {
            'status': status_id
        }
        res = requests.post('https://advert-api.wb.ru/adv/v1/promotion/adverts', headers=headers, params=params)
        res.raise_for_status()
        try:
            camps.append(res.json())
        except Exception as e:
            print(e)
            camps.append([])

    campaigns = list(itertools.chain(*camps))
    ti = kwargs['ti']
    ti.xcom_push(key='campaigns', value=campaigns)


def get_campaign_stats(**kwargs):

    hook = S3Hook('aws')

    conn_id = "wb_statistics"
    conn = BaseHook.get_connection(conn_id)
    extras = conn.extra_dejson
    api_token = extras.get("Authorization")

    headers = {
        'Authorization': api_token
    }

    url = 'https://advert-api.wb.ru/adv/v2/fullstats'

    ti = kwargs['ti']
    campaigns = ti.xcom_pull(key='campaigns')
    date_from = kwargs.get('date_from')
    date_to =   kwargs.get('date_to')
    bucket_name = kwargs.get('bucket_name')

    campaign_ids = [c['advertId'] for c in campaigns]


    batches = list(batchify(campaign_ids, 100))
    n_batches = len(list(batches))

    data = []

    for batch in batches:
        task_completed = False
        payload = [
            {
                'id': c, 
                'interval': {
                    'begin': date_from, 
                    'end': date_to}
            } for c in batch]
        
        while not task_completed:
            res = requests.post(url, headers=headers, json=payload)
            try:
                res.raise_for_status()
                task_completed = True
            except Exception as e:
                print('ERROR', e)
                sleep(30)
        data.append(res.json())
        print(f'Done: {len(data)}/{n_batches}')

    camp_data = []

    for batch in data:
        if batch:
            for c in batch:
                for d in c['days']:
                    for a in d['apps']:
                        for nm in a['nm']:
                            nm['app_type'] = a['appType']
                            nm['date'] = d['date']
                            nm['campaign_id'] = c['advertId']
                            nm['campaign_name'] = [camp['name'] for camp in campaigns if camp['advertId'] == c['advertId']][0]
                            nm['campaign_status'] = [camp['status'] for camp in campaigns if camp['advertId'] == c['advertId']][0]
                            camp_data.append(nm)

    df = pd.DataFrame(camp_data)
    df['date'] = pd.to_datetime(df['date']).dt.date

    df = (
        df
        .groupby(['date','campaign_status', 'campaign_name',  'name', 'nmId'])
        .agg(
            campaign_ids = ( 'campaign_id', 'unique'),
            views = ('views', 'sum'),
            clicks = ('clicks', 'sum'),
            ad_spend = ('sum', 'sum'),
            orders = ('orders', 'sum'),
            items_sold = ('shks', 'sum'),
            sum_price = ('sum_price', 'sum')
        )
        .reset_index()
    )

    # Format datetime now to a safe string for filenames
    now_str = datetime.now().strftime("%Y%m%d_%H%M%S")
    file_name = f'ads_{now_str}.csv'
    csv_buffer = io.StringIO()
    df.to_csv(csv_buffer, index=False)

    # Reset pointer to beginning of StringIO buffer before reading
    csv_buffer.seek(0)

    hook.load_string(
        string_data=csv_buffer.getvalue(),
        bucket_name=bucket_name,
        key=file_name,
        replace=True
    )

    # Use XCom to push the bucket name and file name
    ti = kwargs['ti']
    ti.xcom_push(key='bucket_name', value=bucket_name)
    ti.xcom_push(key='file_name', value=file_name)

def process_data(**kwargs):
    """
    Downloads a file from S3 and loads it into a pandas DataFrame.
    """
    ti = kwargs['ti']

    # Retrieve bucket_name and file_name from XCom
    input_data = {
        'stock': {
            'bucket_name':ti.xcom_pull(task_ids='get_stock', key='bucket_name'),
            'file_name':ti.xcom_pull(task_ids='get_stock', key='file_name'),
        },
        'funnel': {
            'bucket_name':ti.xcom_pull(task_ids='get_funnel', key='bucket_name'),
            'file_name':ti.xcom_pull(task_ids='get_funnel', key='file_name'),
        },
        'campaigns': {
            'bucket_name':ti.xcom_pull(task_ids='get_campaign_stats', key='bucket_name'),
            'file_name':ti.xcom_pull(task_ids='get_campaign_stats', key='file_name'),
    
        }
    }

    funnel_df = get_df_from_s3('aws', input_data['funnel']['bucket_name'], input_data['funnel']['file_name'])
    campaigns_df = get_df_from_s3('aws', input_data['campaigns']['bucket_name'], input_data['campaigns']['file_name'])

    df = (
        pd.merge(funnel_df, campaigns_df, how='outer', left_on=('dt', 'nm_id'), right_on=('date', 'nmId'))
        .sort_values(by='dt', ascending=False)
    )

    df['dt'] = pd.to_datetime(df['dt'])
    df['week'] = df['dt'].dt.to_period('W')
    df['month'] = df['dt'].dt.to_period('M')

    int_columns = ['nm_id', 'views', 'clicks', 'orders', 'items_sold', 'openCardCount', 'addToCartCount', 'ordersCount', 'buyoutsCount']
    float_columns = ['ad_spend', 'sum_price', 'ordersSumRub', 'buyoutsSumRub']
    str_columns = ['dt', 'month', 'week', 'item_name', 'campaign_name', 'campaign_ids']

    df[int_columns] = df[int_columns].fillna(0).astype('int32')
    df[float_columns] = df[float_columns].fillna(0).astype('float32')
    df[str_columns] = df[str_columns].fillna('').astype('str')

    df = df[['dt', 'month', 'week', 'nm_id', 'item_name',  'campaign_name', 'campaign_ids', 
            'views', 'clicks', 'ad_spend', 'orders', 'items_sold', 'sum_price',
            'openCardCount', 'addToCartCount', 'ordersCount', 'ordersSumRub', 'buyoutsCount', 'buyoutsSumRub']]

    df.columns = map_colnames(df.columns)

    now_str = datetime.now().strftime("%Y%m%d_%H%M%S")
    file_name = f'merged_{now_str}.csv'
    bucket_name = kwargs.get('bucket_name')
    csv_buffer = io.StringIO()
    df.to_csv(csv_buffer, index=False)

    # Reset pointer to beginning of StringIO buffer before reading
    csv_buffer.seek(0)

    hook = S3Hook('aws')
    hook.load_string(
        string_data=csv_buffer.getvalue(),
        bucket_name=bucket_name,
        key=file_name,
        replace=True
    )

    # Use XCom to push the bucket name and file name
    ti = kwargs['ti']
    ti.xcom_push(key='bucket_name', value=bucket_name)
    ti.xcom_push(key='file_name', value=file_name)
     



def upload_to_google_sheets(**kwargs):
    hook = GSheetsHook(gcp_conn_id="gc")

    spreadsheet_id = kwargs.get('spreadsheet_id')
    range = kwargs.get('range')
    range_all = kwargs.get('range_all')
    
    ti = kwargs['ti']
    bucket_name = ti.xcom_pull(task_ids='process_data', key='bucket_name')
    file_name = ti.xcom_pull(task_ids='process_data', key='file_name')

    df = get_df_from_s3('aws', bucket_name, file_name).fillna('')
    headers= df.columns.values.tolist()
    values = df.values.tolist()
    
    hook.clear(spreadsheet_id, range_all)
    hook.update_values(spreadsheet_id, range, [headers] + values)

with DAG(
    dag_id='api_dag',
    schedule_interval='0 * * * *',
    start_date=datetime(2024,2,28),
    catchup=False
) as dag:
    
    task_get_stock = PythonOperator(
        task_id='get_stock',
        python_callable=get_stock,
        op_kwargs=global_kwargs
    )

    task_get_funnel = PythonOperator(
        task_id='get_funnel',
        python_callable=get_funnel,
        op_kwargs=global_kwargs
    )

    task_get_campaigns = PythonOperator(
        task_id='get_campaigns',
        python_callable=get_campaigns,
        op_kwargs=global_kwargs
    )

    task_get_campaign_stats = PythonOperator(
        task_id='get_campaign_stats',
        python_callable=get_campaign_stats,
        op_kwargs=global_kwargs
    )

    task_process_data = PythonOperator(
        task_id='process_data',
        python_callable=process_data,
        op_kwargs=global_kwargs,
    )

    task_upload_to_google_sheets = PythonOperator(
        task_id='upload_to_google_sheets',
        python_callable=upload_to_google_sheets,
        op_kwargs=global_kwargs,
    )



    task_get_campaigns >> task_get_campaign_stats
    task_get_stock >> task_get_funnel
    task_get_campaigns >> task_get_campaign_stats
    [task_get_funnel, task_get_campaign_stats] >> task_process_data
    task_process_data >> task_upload_to_google_sheets
