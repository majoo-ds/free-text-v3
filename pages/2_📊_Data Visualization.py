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

############ Big QUERY
# Create API client.
credentials = service_account.Credentials.from_service_account_info(
    st.secrets["gcp_service_account"], scopes=["https://www.googleapis.com/auth/cloud-platform"]
)
client = bigquery.Client(credentials=credentials)

################## GCS
@st.experimental_memo(ttl=24*60*60)
def fetch_db_crm_1():
        dates = ["submit_at", "assign_at", "approved_paid_at", "created_payment","last_update"]
        dtypes = {
            "mt_preleads_code": "category",
            "mt_leads_code": "category",
            "type": "category",
            "campaign_name": "category",
            "assigner": "category",
            "email_sales": "category",
            "m_status_code": "category",
            "outlet_name": "category",
            "owner_phone": "category",
            "rating": "float32",
            "pic_name": "category",
            "full_name": "category",
            "status": "uint8",
            "m_sourceentry_code": "category",
            "counter_followup": "float32",
            "counter_meeting": "float32",
            "channel_name": "category",
            "reject_reason": "category",
            "reject_note": "category"
        }
        
        data = pd.read_csv("gs://lead-analytics-bucket/crm_db/leads_crm.csv",
            storage_options={'token': credentials}, 
            low_memory=False, 
            parse_dates=dates, 
            dtype=dtypes) # read data frame from csv file

        dataframe = data

        # normalize date
        dataframe["submit_at"] = dataframe["submit_at"].dt.normalize()
        dataframe["assign_at"] = dataframe["assign_at"].dt.normalize()
        dataframe["approved_paid_at"] = dataframe["approved_paid_at"].dt.normalize()
        dataframe["created_payment"] = dataframe["created_payment"].dt.normalize()
        dataframe["last_update"] = dataframe["last_update"]

        # cold, hot, warm
        dataframe["leads_potensial_category"] = dataframe.apply(lambda row: 
                                                                        "Cold Leads" if row["rating"] == 1 or row["rating"] == 0
                                                                        else "Warm Leads" if row["rating"] == 2 or row["rating"] == 3
                                                                        else "Hot Leads" if row["rating"] == 4 or row["rating"] == 5
                                                                        else "Null", axis=1)
        # hw by rating
        dataframe["hw_by_rating"] = dataframe.apply(lambda row:
                                                    "hw" if row["leads_potensial_category"] == "Warm Leads"
                                                    else "hw" if row["leads_potensial_category"] == "Hot Leads"
                                                    else "cold" if row["leads_potensial_category"] == "Cold Leads"
                                                    else "Null", axis=1)


        # unnassign, backlog, assigned, junked
        dataframe["status_code"] = dataframe.apply(lambda row:
                                                            "unassigned" if row["status"] == 1
                                                            else "backlog" if row["status"] == 2
                                                            else "assigned" if row["status"] == 3
                                                            else "junked", axis=1)

        # total activity
        dataframe["total_activity"] = dataframe["counter_meeting"] + dataframe["counter_followup"]

        # pipeline
        dataframe["pipeline_by_activity"] = dataframe.apply(lambda row:
                                                        "Pipeline Hot" if "INVOICE" in str(row["m_status_code"])
                                                        else "Pipeline Hot" if row["leads_potensial_category"] == "Hot Leads"
                                                        else "Pipeline Warm" if row["total_activity"] >=2
                                                        else "Pipeline Cold" if row["total_activity"] <=1
                                                        else "Pipeline Null", axis=1)

        # hw by activity
        dataframe["hw_by_activity"] = dataframe.apply(lambda row:
                                                        "hw" if row["pipeline_by_activity"] == "Pipeline Hot"
                                                        else "hw" if row["pipeline_by_activity"] == "Pipeline Warm"
                                                        else "cold" if row["pipeline_by_activity"] == "Pipeline Cold"
                                                        else "Null", axis=1)
                                                                

        # deal or no deal
        dataframe["deal"] = dataframe.apply(lambda row: 
                                                    "deal" if "PAYMENT" in str(row["m_status_code"])
                                                    else "pipeline" if "INVOICE" in str(row["m_status_code"])
                                                    else "deal" if row["m_status_code"] == "PAID"
                                                    else "leads", axis=1)

        

        # filter only campaign and retouch
        # dataframe = dataframe.loc[dataframe["type"] == "campaign"].copy()

        
        

        # remove duplicates
        dataframe.drop_duplicates(subset=["mt_preleads_code"], inplace=True)
        
        return dataframe

######################## CRM DATA FRAME #########################
# run function
df_all = fetch_db_crm_1()


# formatting phone number
df_all['owner_phone'] = df_all['owner_phone'].astype('str')
df_all["owner_phone"] = df_all.apply(lambda row:
                                                "62" + row["owner_phone"][:] if row["owner_phone"].startswith("8")
                                                else row["owner_phone"].replace("0", "62", 1) if row["owner_phone"].startswith("0")
                                                else row["owner_phone"].replace(row["owner_phone"][0:3], "62", 1) if row["owner_phone"].startswith("620")
                                                else row["owner_phone"], axis=1)


################################ DATE RANGE SELECTION #################################
# end date selection
def enddate(date):
    if date.day <=3:
        return datetime.datetime.today()+ datetime.timedelta(-3)
    else:
        return datetime.datetime.today() + datetime.timedelta(-1)
        
run_enddate = enddate(datetime.datetime.today())

# start date selection
def startdate(date):
    if date.day <=3:
        return datetime.datetime.today() + datetime.timedelta(-30)
    else:
        return datetime.datetime.today().replace(day=1)

run_startdate = startdate(datetime.datetime.today())


# select date
with st.sidebar:
    st.markdown(" #### Date Selection")
    select_start_date = st.date_input("Start date", value=run_startdate)
    select_end_date = st.date_input("End date", value=run_enddate)

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
                                else 'tiktok' if str(row['campaign_name']).startswith('regtiktok')
                                else 'facebook' if str(row['campaign_name']).startswith('reg')
                                else 'undefined', axis=1
                            )

df_filtered = dataframe.loc[(pd.to_datetime(dataframe['create_date']).dt.date >= st.session_state['start_date']) & (pd.to_datetime(dataframe['create_date']).dt.date <= st.session_state['end_date'])]
df_filtered['create_date'] = pd.to_datetime(df_filtered['create_date'], errors='coerce')
df_filtered['create_date'] = df_filtered['create_date'].dt.normalize()



# formatting phone number
df_filtered['phone'] = df_filtered['phone'].astype('str')
df_filtered["phone"] = df_filtered.apply(lambda row:
                                                "62" + row["phone"][:] if row["phone"].startswith("8")
                                                else row["phone"].replace("0", "62", 1) if row["phone"].startswith("0")
                                                else row["phone"].replace(row["phone"][0:3], "62", 1) if row["phone"].startswith("620")
                                                else row["phone"], axis=1)

# adset
df_filtered['adset'] = df_filtered.apply(lambda row: 
                                        row['campaign_name'].split('-')[1] if row['campaign_source'] == 'google'
                                        else row['campaign_name'].split('-')[0], axis=1
                                    )

# remove undefined 
df_filtered = df_filtered.loc[df_filtered['campaign_source'] != 'undefined'].copy()

# length of filtered data
len_filtered = len(df_filtered)
# lenght of unique numbers
len_unique = df_filtered['phone'].nunique()

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




df_filtered_grouped = df_filtered.groupby(['campaign_source','adset', 'selected'])['phone'].count().to_frame().reset_index()
df_filtered_grouped.columns = ['campaign_source', 'adset', 'selected', 'count']


############################### MERGE CRM AND FREE TEXT DATA #############################
# slicing crm data based on date filtered
df_all_slice = df_all.loc[(df_all['submit_at'].dt.date >= st.session_state['start_date']) & (df_all['submit_at'].dt.date <= st.session_state['end_date']), ['mt_leads_code', 'owner_phone', 'm_status_code', 'deal']].copy()


# merge dataframe
df_merged = pd.merge(df_filtered, df_all_slice, how='left', right_on='owner_phone', left_on='phone')
# remove duplicates on mt_leads_code
df_merged.drop_duplicates(subset=['mt_leads_code'], inplace=True)
# remove null values
df_merged = df_merged.loc[df_merged['mt_leads_code'].notnull()].copy()
# length of merged data (into CRM)
len_crm = len(df_merged)
# length of deal data
len_deal = len(df_merged.loc[df_merged['deal'] == 'deal'])
# length of pipeline
len_pipeline = len(df_merged.loc[df_merged['deal'] == 'pipeline'])

####### DATAFRAME I
df_merged_grouped = df_merged.groupby(['adset', 'selected', 'm_status_code'])['mt_leads_code'].count().to_frame().reset_index()
df_merged_grouped.columns = ['adset', 'selected', 'status', 'count']

####### DATAFRAME II
df_merged_grouped_deal = df_merged.groupby(['adset', 'selected', 'deal'])['mt_leads_code'].count().to_frame().reset_index()
df_merged_grouped_deal.columns = ['adset', 'selected', 'deal', 'count']

######################## DATA VISUALIZATION #########################
st.markdown("# Campaign Performance")
st.markdown(f'Date range from __{st.session_state["start_date"]}__ to __{st.session_state["end_date"]}__')
# metric cards in columns
col1, col2, col3, col4, col5 = st.columns(5)

# metrics
col1.metric("All Text Campaigns", value=f"{len_filtered:,}")
col2.metric("Unique Phone Numbers", value=f"{len_unique:,}")
col3.metric("Campaign-to-CRM", value=f"{len_crm:,}")
col4.metric("CRM-to-Pipeline", value=f"{len_pipeline:,}")
col5.metric("CRM-to-Deal", value=f"{len_deal:,}")
st.markdown(f"Deal Conversion: __{len_deal/len_filtered:.2%}__")

############### SUNBURST SECTION PART 1 #############

st.subheader("Sunburst Visualization (Campaign Level)")
sunburst_fig = px.sunburst(df_filtered_grouped, path=['campaign_source', 'selected'], values='count', title=f'Date range from {st.session_state["start_date"]} to {st.session_state["end_date"]}', 
                            color_discrete_sequence=px.colors.qualitative.Pastel2, width=600, height=600)

sunburst_fig.update_traces(textinfo="label+percent parent", textfont_size=16)
st.plotly_chart(sunburst_fig, use_container_width=True)

############### SUNBURST SECTION PART 2 #############
st.subheader("Sunburst Visualization (Adset Level)")
sunburst_fig_adset = px.sunburst(df_filtered_grouped, path=['campaign_source', 'adset', 'selected'], values='count', title=f'Date range from {st.session_state["start_date"]} to {st.session_state["end_date"]}', 
                            color_discrete_sequence=px.colors.qualitative.Pastel2, width=600, height=600)

sunburst_fig_adset.update_traces(textinfo="label+percent parent", textfont_size=16)
st.plotly_chart(sunburst_fig_adset, use_container_width=True)

############### SUNBURST SECTION PART 3 #############
st.subheader("Sunburst Visualization (Current Lead Status)")
st.write(f"CRM Data Updated At: {df_all.sort_values(by='last_update', ascending=False).iloc[0]['last_update']}")
sunburst_fig_status = px.sunburst(df_merged_grouped, path=['adset', 'status'], values='count', title=f'Date range from {st.session_state["start_date"]} to {st.session_state["end_date"]}', 
                            color_discrete_sequence=px.colors.qualitative.Pastel2, width=600, height=600)

sunburst_fig_status.update_traces(textinfo="label+percent parent", textfont_size=16)
st.plotly_chart(sunburst_fig_status, use_container_width=True)

############### SUNBURST SECTION PART 4 #############
st.subheader("Sunburst Visualization (Deal Status)")
st.write(f"CRM Data Updated At: {df_all.sort_values(by='last_update', ascending=False).iloc[0]['last_update']}")

sunburst_fig_deal = px.sunburst(df_merged_grouped_deal, path=['adset', 'deal'], values='count', title=f'Date range from {st.session_state["start_date"]} to {st.session_state["end_date"]}', 
                            color_discrete_sequence=px.colors.qualitative.Pastel2, width=600, height=600)

sunburst_fig_deal.update_traces(textinfo="label+percent parent", textfont_size=16)
st.plotly_chart(sunburst_fig_deal, use_container_width=True)

############### LINE CHART SECTION #############
st.subheader("Daily Counts All Leads")
daily_grouped = df_filtered.groupby(['create_date', 'campaign_source', 'selected'])['phone'].count().to_frame().reset_index()

# rename columns
daily_grouped.columns = ['date', 'campaign_source', 'selected', 'count']

daily_fig = px.line(daily_grouped, x='date', title=f'Daily Number of incoming free text campaign from{st.session_state["start_date"]} to {st.session_state["end_date"]}',
                    y='count', color='campaign_source', color_discrete_map={'facebook': '#6495ED', 'google': '#006400'}, line_dash='selected') 

daily_fig.update_traces(textposition="bottom right", texttemplate='%{text:,}', textfont_size=16)
daily_fig.update_layout({
'plot_bgcolor': 'rgba(0, 0, 0, 0)',
'paper_bgcolor': 'rgba(0, 0, 0, 0)',
}, yaxis=dict(
    showgrid=True,
    zeroline=True,
    showline=True,
    showticklabels=True,
),
    legend=dict(
    yanchor="top",
    y=0.99,
    xanchor="left",
    x=0.99
), width=1000)
daily_fig.update_xaxes(
    title_text = "Date",
    title_standoff = 25,
    linecolor='rgb(204, 204, 204)',
    linewidth=2,
    ticks='outside',
    tickfont=dict(
        family='Arial',
        size=12,
        color='rgb(82, 82, 82)',
    ))
st.plotly_chart(daily_fig, use_container_width=True)


############### LINE CHART SECTION #############
st.subheader("Daily Counts By Selected")

daily_grouped_all = df_filtered.groupby(['create_date', 'selected'])['phone'].count().to_frame().reset_index()
# rename columns
daily_grouped_all.columns = ['date', 'selected', 'count']

daily_fig_all = px.line(daily_grouped_all, x='date', title=f'Daily Number of incoming free text campaign from{st.session_state["start_date"]} to {st.session_state["end_date"]}',
                    y='count', color='selected', text='count') 

daily_fig_all.update_traces(textposition="bottom right", texttemplate='%{text:,}', textfont_size=16)
daily_fig_all.update_layout({
'plot_bgcolor': 'rgba(0, 0, 0, 0)',
'paper_bgcolor': 'rgba(0, 0, 0, 0)',
}, yaxis=dict(
    showgrid=True,
    zeroline=True,
    showline=True,
    showticklabels=True,
),
    legend=dict(
    yanchor="top",
    y=0.99,
    xanchor="left",
    x=0.99
), width=1000)
daily_fig_all.update_xaxes(
    title_text = "Date",
    title_standoff = 25,
    linecolor='rgb(204, 204, 204)',
    linewidth=2,
    ticks='outside',
    tickfont=dict(
        family='Arial',
        size=12,
        color='rgb(82, 82, 82)',
    ))
st.plotly_chart(daily_fig_all, use_container_width=True)
