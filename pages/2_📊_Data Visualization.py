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
import plotly.express as px

st.set_page_config(layout="wide", page_title="Free Text Analysis", page_icon="🎭")
st.markdown("# Free Text Data Visualization")

# Big QUERY
# Create API client.
credentials = service_account.Credentials.from_service_account_info(
    st.secrets["gcp_service_account"]
)
client = bigquery.Client(credentials=credentials)

################################ DATE RANGE SELECTION #################################
# select date
with st.sidebar:
    st.markdown(" #### Date Selection")
    select_start_date = st.date_input("Start date", value=datetime.datetime.today().replace(day=1))
    select_end_date = st.date_input("Start date", value=datetime.datetime.today() + datetime.timedelta(-1))

############ session state
# initiate session_state for date selection
if "start_date" not in st.session_state:
    st.session_state["start_date"] = select_start_date

# initiate session_state for date selection
if "end_date" not in st.session_state:
    st.session_state["end_date"] = select_end_date

# create submit button
change_date = st.sidebar.button('Change date')

# when button is clicked, then update states
if change_date:
    st.session_state["start_date"] = select_start_date
    st.session_state["end_date"] = select_end_date




################################ FETCH DATA FROM BIGQUERY #################################
query = 'SELECT * FROM `teak-advice-354202.free_text_v3.all_status` ORDER BY create_date DESC'
# filter = f' WHERE create_date >=  {st.session_state["start_date"].strftime("%Y-%m-%d")} AND create_date <= {st.session_state["end_date"].strftime("%Y-%m-%d")}'
# order = ' ORDER BY created_date DESC'
# sql = query + filter + order

@st.cache(allow_output_mutation=True, ttl=10*60*60)
def get_bigquery():
    df = pandas_gbq.read_gbq(query, project_id="teak-advice-354202", credentials=credentials)

    return df

# run get_bigquery
dataframe = get_bigquery()

# data manipulation
dataframe['campaign_source'] = dataframe.apply(lambda row: 
                                'google' if str(row['campaign_name']).startswith('ggl')
                                else 'facebook' if str(row['campaign_name']).startswith('reg')
                                else 'undefined', axis=1
                            )

df_filtered = dataframe.loc[(pd.to_datetime(dataframe['create_date']).dt.date >= st.session_state['start_date']) & (pd.to_datetime(dataframe['create_date']).dt.date <= st.session_state['end_date'])]

############### AG GRID ################
# update and return mode
return_mode_value = DataReturnMode.__members__["FILTERED"]
update_mode_value = GridUpdateMode.__members__["GRID_CHANGED"]

# grid options of AgGrid
gb = GridOptionsBuilder.from_dataframe(df_filtered)
gb.configure_pagination(enabled=True, paginationAutoPageSize=False, paginationPageSize=20)
gridOptions = gb.build()    

grid_response = AgGrid(
    df_filtered, 
    gridOptions=gridOptions,
    data_return_mode=return_mode_value, 
    update_mode=update_mode_value,
    theme='streamlit')




df_filtered_grouped = df_filtered.groupby(['campaign_source', 'selected'])['phone'].count().to_frame().reset_index()
df_filtered_grouped.columns = ['campaign_source', 'selected', 'count']

######################## DATA VISUALIZATION #########################
st.markdown("# Campaign Performance")
st.subheader("Sunburst Visualization")
sunburst_fig = px.sunburst(df_filtered_grouped, path=['campaign_source', 'selected'], values='count', title=f'Date range from {st.session_state["start_date"]} to {st.session_state["end_date"]}', 
                            color_discrete_sequence=px.colors.qualitative.Pastel2, width=600, height=600)

sunburst_fig.update_traces(textinfo="label+percent parent")
st.plotly_chart(sunburst_fig, use_container_width=True)