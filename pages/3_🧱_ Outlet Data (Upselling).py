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

st.set_page_config(layout="wide", page_title="Outlet Data for Upselling", page_icon="🛒")
st.markdown("# Outlet Data")

# Big QUERY and GCS Client
# Create API client.
credentials = service_account.Credentials.from_service_account_info(st.secrets["gcp_service_account"], scopes=["https://www.googleapis.com/auth/cloud-platform"])
client = bigquery.Client(credentials=credentials)

@st.experimental_memo(ttl=1*60*60)
def get_outlet_data():
    dataframe = pd.read_csv("gs://lead-analytics-bucket/crm_db/outlet_data.csv",
        storage_options={'token': credentials})
    dataframe = dataframe.iloc[:, 1:].copy()
    
    return dataframe

# run dataframe
dataframe = get_outlet_data()

# Explanation
with st.expander("How to filter the data"):
        st.markdown(
            """
            Steps:
        
            __1. Select necessary columns__
            
            To focus on filtering the data, just select the needed columns and unselect others
            
            __2. Filter each column:__
            
            Narrow down the selected data by filtering the required criterias
            
            __3. Download data:__
            
            After finished, do not forget to download for further processing
            
        
        """
        )

############## AG GRID ###################

# update and return mode
return_mode_value = DataReturnMode.__members__["FILTERED"]
update_mode_value = GridUpdateMode.__members__["GRID_CHANGED"]

# grid options of AgGrid
gb = GridOptionsBuilder.from_dataframe(dataframe)
gb.configure_selection(selection_mode='multiple', use_checkbox=True, groupSelectsChildren=True, groupSelectsFiltered=True)
gb.configure_default_column(enablePivot=True, enableValue=True, enableRowGroup=True)   
gb.configure_pagination(enabled=True, paginationAutoPageSize=False, paginationPageSize=20)
gridOptions = gb.build()    

grid_response = AgGrid(
    dataframe, 
    gridOptions=gridOptions,
    enable_enterprise_modules=True,
    update_mode=GridUpdateMode.MODEL_CHANGED,
    data_return_mode=DataReturnMode.FILTERED_AND_SORTED,
    fit_columns_on_grid_load=False,
    header_checkbox_selection_filtered_only=True,
    use_checkbox=True,
    theme='streamlit')



# title
st.markdown('# Selected Data')

# selected rows
df_selected = pd.DataFrame(grid_response['selected_rows'])
st.dataframe(df_selected)

# title
st.markdown('# Downloadable Data')

# download the dataframe
buffer = io.BytesIO()
with pd.ExcelWriter(buffer, engine='xlsxwriter') as writer:
    # Write excel with single worksheet
    df_selected.to_excel(writer, index=False)
    # Close the Pandas Excel writer and output the Excel file to the buffer
    writer.save()

    # assign file to download button
    st.download_button(
        label="Download Data in Excel",
        data=buffer,
        file_name=f"outlet_data_for_upsell_{datetime.datetime.now().strftime('%Y-%m-%d')}.xlsx",
        mime="application/vnd.ms-excel"
)