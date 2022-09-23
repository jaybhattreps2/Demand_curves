# -------------------------------------------------------------------------------- NOTEBOOK-CELL: CODE
# -*- coding: utf-8 -*-
import dataiku
import mysql.connector
from tqdm import tqdm
from datetime import timedelta,datetime
import pandas as pd, numpy as np
from dataiku import pandasutils as pdu
from google.cloud import bigquery
from google.oauth2 import service_account

lobsang_con = mysql.connector.connect(host="reps-ods-cluster-1.cluster-ro-curqk9xgnt28.us-east-2.rds.amazonaws.com",
  port='3306',
  database ='lobsang',
  user="jaybhatt",
  password="72QmYetSNfGM")

kill_shot_con = mysql.connector.connect(
  host="killshot-ods-cluster.cluster-ro-curqk9xgnt28.us-east-2.rds.amazonaws.com",
  port='3306',
  database ='killshot',
  user="jaybhatt",
  password="72QmYetSNfGM")

# -------------------------------------------------------------------------------- NOTEBOOK-CELL: CODE
credentials = service_account.Credentials.from_service_account_info({
  "type": "service_account",
  "project_id": "stubhub-bigquery",
  "private_key_id": "9cefe02d275edb34422a47292511ff90c57608de",
  "private_key": "-----BEGIN PRIVATE KEY-----\nMIIEvAIBADANBgkqhkiG9w0BAQEFAASCBKYwggSiAgEAAoIBAQC6pJMDuOZbaq81\nXR1te+QYh4oDyFJrSVS1txyR7N7UI0WO5GEQzOPTQ7lMUutTy0IKW4tQZ/tHUMsR\nY4CV2oK6eHXkraCzw6JPbEoB8nBcDbS4R0ytwhA351Mcy5GshSzu3DASkC8Nxx7y\nOXrWNTVWB51gy9nxsWTZegV5qPx3H+oiyd+y/R6ZSiYTLkEIZWETuNKz5oVMS06C\n3Fv3Gv+PJroi+RPyfE7gC8ubPGGnbe7ACeYUWynGy7GiTyJ1FXKtTcB8UtMVIwwK\npjBNL6p9ra3ZEKf3242YJ7qKUSdzwqrMk9cNJ3r7hfaZToFKUE95ERFYSSeKcTyK\nwmr9HbQ3AgMBAAECggEAKDJakUIopoaW/nyz4kj86oWCZWcmzQFpk4tAxXLI2NJR\n4EDyYL5n0K+0wCnZkd3kNrvZiLMkTIsreO6/mkaJwiRAa8QzKJQZKFbPp5Sxuzme\nctO0QXXarVeOMFTtZkT2VOkqF01jPOnmEc/sUyb5ejJApECzCxkj+ayMclPBL9xb\nEN2PTTGzj4DDNqhgvFKEzV/9/qS1mq1tfu2GJ32iE7ve8GskAumXH7/qJrVjqtJ+\nWdkkR96PjCTf/FE0inFPfrQJp2Fv4geo3YGgV6ReTfrteBtpOtk8dm3qpe37QkGo\nyfPyFhvSRnk2j2B1ygVbUWxNXyccOvhGlhHe3B9CzQKBgQDfaaSZRT8zAXfNQTsk\nirJGYEdzoj1I8xC68AVsRE/5VaubxJIrOM5VOPMbc0HLzrnJGmUH9ex2hs2BLUHC\njo+BlgGH5cWvOWmobUJ+OBUyeYcpxaKL7+FKTrVC9HGWxhS0rWWb3nUaQPg+dOHs\nCmk6p3IsHezv2jQSpqJX83ReYwKBgQDV3e0JgnteoCsGljHEz7Gv9JGfOj1gmSBP\ndhdsGjdbjeJYKIOvb/xPU9hdnqtKfa2o6Pz8WorlcQK2U2LEmLUeQwtkPwluRHnP\neYqDKk1arDUJKtasp2WMgSYys9V6M7BkGILHISG3W0c1q1/7zHb1f+Q4XtpbJULb\nlfaKgj7hHQKBgHtucnH7wB7+AKJ2F6boufYH23IXgKR9JhEh6t0WINkwa89zmw4m\nJFkRTb8svn1LiXmCC6+KP0p8z60+w8Yp3T9LES1z1PQqVCWpt8LkWVG5suNxPYzy\nyxWyxpJnWxph9a6c3jZvgWMv4fcfvHIcjmbJfFrDbdRWSc7EIY1WBEThAoGAKxuy\nncoZR/eM9KTtnzgmHstzVt8MZB45bwrkqbuXEPNGfnKcKI6wEuVZpXVIHZm7mWJt\nLAisGpdu7oVcUTheuZzV/Pzfz8QpsXJUQyARu4ceoZxq7R5Iz8twgaPSEfG9Sk/O\nfIjnHOhfMCg9DqomCFIhFGO6K8kU8uIFceHS/tkCgYBUMj+bVEBJEb9ua4IqsZ60\n4vyKBrnJyI2meU1AVsHE5CzG5ioTxSAm6uAZ4sykaRQNHM1qSp5HQF7dz1pGFFcE\nJiLs5WYFfSpxxzTmE3wrPyaYJh7/FzFqGhMEmKAyjnBDMtRW8X4iNZrgpqOSpvq2\nEHL3Oz2JilGXYLpb8zX7UA==\n-----END PRIVATE KEY-----\n",
  "client_email": "jaybhatt-reps2-local@stubhub-bigquery.iam.gserviceaccount.com",
  "client_id": "114763199820593651814",
  "auth_uri": "https://accounts.google.com/o/oauth2/auth",
  "token_uri": "https://oauth2.googleapis.com/token",
  "auth_provider_x509_cert_url": "https://www.googleapis.com/oauth2/v1/certs",
  "client_x509_cert_url": "https://www.googleapis.com/robot/v1/metadata/x509/jaybhatt-reps2-local%40stubhub-bigquery.iam.gserviceaccount.com"
})
client = bigquery.Client(credentials= credentials,project=credentials.project_id)


# Read recipe inputs
demand_curves_v3 = dataiku.Dataset("demand_curves_v3")
demand_curve_data = demand_curves_v3.get_dataframe()

demand_curve_data = pd.read_gbq('stubhub-bigquery.revenue_management.demand_curves_v3',
                                  project_id=credentials.project_id,
                                  credentials=credentials,
                                 )

# # Compute recipe outputs from inputs
# # TODO: Replace this part by your actual code that computes the output, as a Pandas dataframe
# # NB: DSS also supports other kinds of APIs for reading and writing data. Please see doc.

# -------------------------------------------------------------------------------- NOTEBOOK-CELL: CODE
## Reading the upcoming events from killshot
get_up_cm_event = '''select
Date_add(e.datetime, interval -8 hour) as "event_datetime",
e.lobsang_event_id,e.genre,e.category ,e.performer,e.venue
from event e where date(e.datetime) >= CurDate()'''
up_event = pd.read_sql(get_up_cm_event, con=kill_shot_con)
# up_event.head()

venue_type = pd.read_sql('''SELECT v.name,vt.type  FROM venue v
join venue_type vt on v.type = vt.id''',con= lobsang_con)


up_event = up_event.merge(venue_type,left_on='venue',right_on='name')

### Considering next 30 days events

# up_event = up_event[up_event['event_datetime'] < (datetime.today()+timedelta(days= 30))]
up_event = up_event[up_event['event_datetime'] > (datetime.today())]

up_event = up_event.drop(['venue','name'],1)

target_dte = pd.read_gbq('stubhub-bigquery.revenue_management.target_sellout_dte',
                            project_id=credentials.project_id,
                            credentials=credentials)

# -------------------------------------------------------------------------------- NOTEBOOK-CELL: CODE

target_dte.genre =target_dte.genre.str.replace('K-pop','Pop')
target_dte.head()

# -------------------------------------------------------------------------------- NOTEBOOK-CELL: CODE
def create_target_sales(event_id,pushout_dte = 0):
    #####################
    event_df = pd.read_gbq(f'''select * from stubhub-bigquery.revenue_management.future_events where
                       event_id = {event_id} and cancelled = False''',
                                  project_id=credentials.project_id,
                                  credentials=credentials)

    event_df.event_datetime  =event_df.event_datetime.dt.tz_localize(None)
    event_df.sale_date  =event_df.sale_date.dt.tz_localize(None)
    event_df.purchase_date  =event_df.purchase_date.dt.tz_localize(None)

    event_date = event_df.event_datetime[0]
    genre = event_df.genre.iloc[0]

    all_hist = event_df.groupby('sale_date')[['sold_ind']].count().join(event_df.groupby('purchase_date')[['ticket_id']].count(),how='outer').fillna(0)
    all_hist =all_hist.rename(columns={'sold_ind':'Sold_tickets','ticket_id':'Purchased_tickets'})
    all_hist = all_hist[['Purchased_tickets','Sold_tickets']]


    ###################
    ### Here looking for the demand curve of genre
    for col in demand_curve_data.columns[:-1]:
        demand_curve_data[col]=pd.to_numeric(demand_curve_data[col],errors='coerce')

    dc_gern =demand_curve_data[demand_curve_data.genre == genre].sort_values('DTE').reset_index(drop=True)
    ### tatal purchage vs sales
    total_pur,total_sold = all_hist.sum().tolist()
    # current DTE for given event
    current_DTE = -(event_date - datetime.today()).days
    #
    demand_fu =dc_gern.set_index('DTE').loc[current_DTE:][['Depriced_Cumulative_percentage']]


    target_sold = demand_fu['Depriced_Cumulative_percentage'].iloc[0]*total_pur

    diff = target_sold-total_sold
    pace = (demand_fu['Depriced_Cumulative_percentage'] - demand_fu['Depriced_Cumulative_percentage'].shift(1)).fillna(0)
    pace_adjust =pace*(1/pace.sum())*diff
    target_pace =(demand_fu['Depriced_Cumulative_percentage'] - demand_fu['Depriced_Cumulative_percentage'].shift(1).fillna(0))*total_pur
    target_pace.iloc[0]= total_sold
    ###### adjusting the pace here
    demand_fu['target_sale']= pace_adjust + target_pace
    demand_fu['target_sale_cum'] = demand_fu['target_sale'].cumsum()
    demand_fu['Ideal_sales'] = demand_fu['Depriced_Cumulative_percentage']*total_pur


    ## Adding past history here

    ## Adding DTE in the all historical table here
    all_hist['DTE'] = (all_hist.index-event_date).days


    # ## checking for the min date transation available with the all historical table
    min_dte = min((all_hist.index-event_date).days)

    # ###As we may have mutiple transaction in a one day hence summing them all together
    all_hist = all_hist.groupby('DTE').sum()

    #### Creating a data frame with daily purchage and sales for the event

    int_df = pd.DataFrame(index = [i for i in range(min_dte,1)])
    int_df = int_df.join(all_hist)
    int_df['diff'] =  int_df['Purchased_tickets']-int_df['Sold_tickets']
    int_df['remaining']= int_df['diff'].fillna(0).cumsum()
    former_sales = int_df.copy()

    min_index = dc_gern.set_index('DTE').index.min()

    ## expanding demand curve for more them 200 days data
    if min_index > min_dte:
        int_df = pd.DataFrame(index = [i for i in range(min_dte,1)])
        int_df = int_df.join(dc_gern.set_index('DTE')['Depriced_Cumulative_percentage'])
        int_df['Depriced_Cumulative_percentage'] = int_df['Depriced_Cumulative_percentage'].replace(0,np.nan)
        int_df.loc[min_dte] = 0
        int_df['Depriced_Cumulative_percentage']= int_df['Depriced_Cumulative_percentage'].interpolate()
        int_df = int_df.join(former_sales)
    else:
    #     if we want demand curves for less then 200 days
        int_df = former_sales.join(dc_gern.set_index('DTE')['Depriced_Cumulative_percentage'])
        int_df['Depriced_Cumulative_percentage']= int_df['Depriced_Cumulative_percentage'].interpolate()

    ### updating the scale down logic for those event where first sale point fall within 200 days
    if min(int_df.index)>-200:
       new_scr =  (int_df['Depriced_Cumulative_percentage']/(int_df['Depriced_Cumulative_percentage'].iloc[-1]-
                                                              int_df['Depriced_Cumulative_percentage'].iloc[0]))
       scaled_scr = new_scr- new_scr.iloc[0]
       int_df['Depriced_Cumulative_percentage']=scaled_scr


    int_df['Ideal Sales based on demand Curve'] = int_df['Depriced_Cumulative_percentage']*total_pur

    int_df['Actual Past Sales'] = int_df['Sold_tickets'].fillna(0).cumsum()

    ## joining all tables to get required data you can get additional data in future from here
    int_df =int_df[['Actual Past Sales']].loc[:current_DTE].join(demand_fu[['target_sale_cum']],how='outer').join(int_df[['Ideal Sales based on demand Curve']])

    int_df = int_df.rename(columns = {'Actual Past Sales':'Sold_Cume','target_sale_cum':'New_Target',
                            'Ideal Sales based on demand Curve':'Depriced'})

    #####################
    return int_df

### Getting forecast here from the depriced values

def get_forecast(df):
    last_val = df['Sold_Cume'].dropna().iloc[-1]
    last_index = df['Sold_Cume'].dropna().index[-1]
    abs_derpriced = df['Depriced'].loc[last_index:].diff()
    abs_derpriced.loc[last_index]= last_val
    return(df['Sold_Cume'].dropna().append(abs_derpriced.cumsum().loc[last_index+1:]))

# -------------------------------------------------------------------------------- NOTEBOOK-CELL: CODE
# ## Getting the schema to avoid any schema related error going ahead
# table = client.get_table('stubhub-bigquery.revenue_management.demand_curve_future')
# generated_schema = [{'name':i.name, 'type':i.field_type} for i in table.schema]

# -------------------------------------------------------------------------------- NOTEBOOK-CELL: CODE
all_df = pd.DataFrame()

## Keeping only those event which are in next 400 days
up_event = up_event[up_event['event_datetime']< datetime.now()+timedelta(days= 400)].reset_index(drop=True)

total_count = int(len(up_event.lobsang_event_id.unique())/100)
append_count=0
count=1
for event in tqdm(up_event.lobsang_event_id.unique()[1:]):
    _,event,genre,category,_,type = up_event[up_event.lobsang_event_id==event].iloc[0]
    target_sellout_dte = target_dte[(target_dte.genre==genre) & (target_dte.venue_type==type)].target_sellout_dte.values
    if len(target_sellout_dte)==0:
        target_sellout_dte = 1
    else:
        target_sellout_dte = target_sellout_dte[0]


    try:
        df = create_target_sales(event,pushout_dte = target_sellout_dte).reset_index()
        df = df.rename(columns = {'index':'DTE'})
        df['DTE']= df['DTE'].shift(target_sellout_dte)
        df = df.dropna(subset=['DTE'])
        df = df.append(pd.DataFrame(range(int(df['DTE'].max())+1,0+1),columns =['DTE'])).reset_index(drop=True)
        df['New_Target'] = df['New_Target'].ffill()
        df['Depriced'] = df['Depriced'].ffill()
        df['event_id'] = event
        df['forecast'] = get_forecast(df)
        all_df= pd.concat([all_df,df])
    except Exception as E:
        print(E, event)
    try:    
        if count% total_count==0:
           if append_count ==0:
                all_df['DTE'] = all_df['DTE'].astype(int)
                all_df.to_gbq('stubhub-bigquery.revenue_management.demand_curve_future',
                            project_id=credentials.project_id,
                            credentials=credentials,
                            if_exists='replace')
                append_count+=1
           else:
               all_df['DTE'] = all_df['DTE'].astype(int)
               all_df.to_gbq('stubhub-bigquery.revenue_management.demand_curve_future',
                            project_id=credentials.project_id,
                            credentials=credentials,
                            if_exists='append')
               print(all_df.shape)
               all_df = pd.DataFrame()
    except Exception as E:
        print(E, event)
        
    count+=1

        
    
all_df['DTE'] = all_df['DTE'].astype(int)
all_df.to_gbq('stubhub-bigquery.revenue_management.demand_curve_future',
                            project_id=credentials.project_id,
                            credentials=credentials,
                            if_exists='append')

# -------------------------------------------------------------------------------- NOTEBOOK-CELL: CODE
all_df.to_gbq('stubhub-bigquery.revenue_management.demand_curve_future1',
                    project_id=credentials.project_id,
                    credentials=credentials,
                    if_exists='replace')

# -------------------------------------------------------------------------------- NOTEBOOK-CELL: CODE
demand_curve_future_df =  pd.read_gbq('stubhub-bigquery.revenue_management.demand_curve_future',
                            project_id=credentials.project_id,
                            credentials=credentials)

# -------------------------------------------------------------------------------- NOTEBOOK-CELL: CODE
##### Write recipe outputs
demand_curve_future = dataiku.Dataset("demand_curve_future")
demand_curve_future.write_with_schema(demand_curve_future_df)
