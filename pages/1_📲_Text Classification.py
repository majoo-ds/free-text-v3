import streamlit as st
import pandas as pd
import json
import datetime
import requests
import io
from google.oauth2 import service_account
from google.cloud import bigquery
from st_aggrid import GridOptionsBuilder, AgGrid, GridUpdateMode, DataReturnMode
import pandas_gbq
from pandas.api.types import (
    is_categorical_dtype,
    is_datetime64_any_dtype,
    is_numeric_dtype,
    is_object_dtype,
)

st.set_page_config(layout="wide", page_title="Free Text Classification", page_icon="🎭")
st.markdown("# Free Text Classification using Deep Learning")
st.markdown("__Max date is 30 days from today__")


# Big QUERY
# Create API client.
credentials = service_account.Credentials.from_service_account_info(
    st.secrets["gcp_service_account"]
)
client = bigquery.Client(credentials=credentials)



# endpoint
url = 'https://majoo.id/api/report/integration?'


# end date selection
def enddate(date):
    if date.day <=3:
        return datetime.datetime.today()
    else:
        return datetime.datetime.today() + datetime.timedelta(-1)
        
run_enddate = enddate(datetime.datetime.today())

# select date
with st.sidebar:
    st.markdown(" #### Date Selection")
    select_start_date = st.date_input("Start date", value=datetime.datetime.today().replace(day=1), key=1)
    select_end_date = st.date_input("End date", value=run_enddate, key=2)


############ session state
# initiate session_state for date selection
if "start" not in st.session_state:
    st.session_state["start"] = select_start_date

# initiate session_state for date selection
if "end" not in st.session_state:
    st.session_state["end"] = select_end_date

# create submit button
change_date = st.sidebar.button('Change date')

# when button is clicked, then update states
if change_date:
    st.session_state["start"] = select_start_date
    st.session_state["end"] = select_end_date

# creat function to fetch data
st.cache(allow_output_mutation=True, ttl=30*60*60)
def get_data():
    res = requests.get(url + f'startdate={st.session_state["start"]}&' + f'enddate={st.session_state["end"]}')
    dataframe = pd.DataFrame(res.json()['data'])

    return dataframe



# get dataframe
df = get_data()

############## AG GRID ###################

# update and return mode
return_mode_value = DataReturnMode.__members__["FILTERED"]
update_mode_value = GridUpdateMode.__members__["GRID_CHANGED"]

# title
st.markdown('# Main Data')

# grid options of AgGrid
gb = GridOptionsBuilder.from_dataframe(df)
gb.configure_selection(selection_mode='multiple', use_checkbox=True, groupSelectsChildren=True, groupSelectsFiltered=True)   
gb.configure_pagination(enabled=True, paginationAutoPageSize=False, paginationPageSize=20)
gridOptions = gb.build()    

grid_response = AgGrid(
    df, 
    gridOptions=gridOptions,
    data_return_mode=return_mode_value, 
    update_mode=update_mode_value,
    theme='streamlit')

# title
st.markdown('# Selected Data')

# selected rows
selected = pd.DataFrame(grid_response['selected_rows'])
st.dataframe(selected)

# csv format for bulk upload
url_file = 'https://docs.google.com/spreadsheets/d/e/2PACX-1vRsEd3xRwST913ShSJWvQHfJukE9w3uxKUXKgEZS1p0Jfpdev7UOw2nH52phIUAi4eJNSeCDHf0PbHM/pub?output=csv'
@st.cache(allow_output_mutation=True)
def get_bulk_format(url):
    df = pd.read_csv(url)
    # inputting bulk format with data from selected rows
    df['Outlet Name'] = pd.DataFrame(grid_response['selected_rows'])['business_name']
    df['Nama PIC'] = pd.DataFrame(grid_response['selected_rows'])['name']
    df['Email Address'] = pd.DataFrame(grid_response['selected_rows'])['email']
    df['Phone Number'] = pd.DataFrame(grid_response['selected_rows'])['phone']
    df['Notes'] = pd.DataFrame(grid_response['selected_rows'])['reason_need_majoo']
    df['Sub Entry Source'] = pd.DataFrame(grid_response['selected_rows'])['campaign_name']
    df['Entry Source'] = 'MARKETING-CAMPAIGN'
    return df

# run dataframe
bulk_df = get_bulk_format(url_file)



# title
st.markdown('# Downloadable Data')
st.dataframe(bulk_df)

 # download the dataframe
buffer = io.BytesIO()
with pd.ExcelWriter(buffer, engine='xlsxwriter') as writer:
    # Write excel with single worksheet
    bulk_df.to_excel(writer, index=False)
    # Close the Pandas Excel writer and output the Excel file to the buffer
    writer.save()

    # assign file to download button
    st.download_button(
        label="Download Data in Excel",
        data=buffer,
        file_name=f"enhanced-form_bulk-upload{datetime.datetime.now().strftime('%Y-%m-%d')}.xlsx",
        mime="application/vnd.ms-excel"
)


#### UPLOAD TO BIGQUERY

## Selected
st.markdown('# Save Selected to Database')
df_selected = selected
df_selected['selected'] = 'yes'
df_selected = df_selected.iloc[:,1:]
st.dataframe(df_selected)

# button
submit_selected = st.button('Save to DB', key='1')

if submit_selected:
    pandas_gbq.to_gbq(df_selected, "free_text_v3.all_status",
              project_id="teak-advice-354202", if_exists="append", credentials=credentials)

## Not Selected
st.markdown('# Save Non-Selected to Database')
df_not_selected = df.loc[~df['business_name'].isin(df_selected['business_name'].tolist())].copy()
df_not_selected['selected'] = 'no'
st.dataframe(df_not_selected)

# button
submit_not_selected = st.button('Save to DB', key='2')

if submit_not_selected:
    pandas_gbq.to_gbq(df_not_selected, "free_text_v3.all_status",
              project_id="teak-advice-354202", if_exists="append", credentials=credentials)