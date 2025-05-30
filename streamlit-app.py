import streamlit as st
from streamlit_autorefresh import st_autorefresh
import time
import os
from dotenv import load_dotenv
import psycopg2
from kafka import KafkaConsumer
import simplejson as json
import pandas as pd
import matplotlib.pyplot as plt 
import numpy as np

load_dotenv()
host = os.getenv('host')
dbname = os.getenv('dbname')
user = os.getenv('user')
password = os.getenv('password')


def create_kafka_consumer(topic_name):
    consumer = KafkaConsumer(
        topic_name,
        bootstrap_servers='localhost:9092',
        auto_offset_reset='earliest',
        value_deserializer=lambda x:json.loads(x.decode('utf-8'))
    )
    return consumer

def fetch_data_from_kafka(consumer):
    messages = consumer.poll(timeout_ms=1000)
    data = []
    
    for message in messages.values():
        for sub_message in message:
            data.append(sub_message.value)
    
    return data

def plot_bar_chart(data:  pd.DataFrame):
    data_type = data['candidate_name']
    colors = plt.cm.viridis(np.linspace(0, 1, len(data_type)))
    plt.bar(data_type, data['total_votes'], color=colors)
    plt.xlabel('Candidate', fontsize=14)
    plt.ylabel('Total Votes', fontsize=14)
    plt.title('Vote Counts per Candidate', fontsize=14)
    plt.xticks(rotation=90, fontsize=12)
    return plt

def plot_donut_chart(data: pd.DataFrame, title='Voting Result Overview', type='candidate'):
    if type == 'candidate':
        labels = list(data['candidate_name'])
    elif type == 'gender':
        labels = list(data['gender'])

    sizes = list(data['total_votes'])
    fig, ax = plt.subplots()
    ax.pie(sizes, labels=labels, autopct='%1.1f%%', startangle=140, textprops={'fontsize': 12})
    ax.axis('equal')
    plt.title(title, fontsize=14,pad=20)
    return fig
    
# consumer = create_kafka_consumer('aggregated_votes_per_candidate')
# data = fetch_data_from_kafka(consumer)
# data = pd.DataFrame(data)
# results = data.loc[data.groupby('candidate_id')['total_votes'].idxmax()]
# print(results)
@st.cache_data
def fetch_voting_stats():
    conn = psycopg2.connect(f"host={host} dbname={dbname} user={user} password={password}")
    cur = conn.cursor()
    
    cur.execute("""
                SELECT count(*) voters from voters
                """)
    voters_count = cur.fetchone()[0]
    
    cur.execute("""
                SELECT count(*) candidates from candidates
                """)
    candidates_count = cur.fetchone()[0]
    
    return voters_count , candidates_count

@st.cache_data(show_spinner=False)
def split_frame(input_df, rows):
    df = [input_df.loc[i: i + rows - 1, :] for i in range(0, len(input_df), rows)]
    return df
def paginate_table(table_data):
    top_menu = st.columns(3)
    with top_menu[0]:
        sort = st.radio("Sort Data", options=["Yes", "No"], horizontal=1, index=1)
    if sort == "Yes":
        with top_menu[1]:
            sort_field = st.selectbox("Sort By", options=table_data.columns)
        with top_menu[2]:
            sort_direction = st.radio(
                "Direction", options=["⬆️", "⬇️"], horizontal=True
            )
        table_data = table_data.sort_values(
            by=sort_field, ascending=sort_direction == "⬆️", ignore_index=True
        )
    pagination = st.container()

    bottom_menu = st.columns((4, 1, 1))
    with bottom_menu[2]:
        batch_size = st.selectbox("Page Size", options=[10, 25, 50, 100])
    with bottom_menu[1]:
        total_pages = (
            int(len(table_data) / batch_size) if int(len(table_data) / batch_size) > 0 else 1
        )
        current_page = st.number_input(
            "Page", min_value=1, max_value=total_pages, step=1
        )
    with bottom_menu[0]:
        st.markdown(f"Page **{current_page}** of **{total_pages}** ")

    pages = split_frame(table_data, batch_size)
    pagination.dataframe(data=pages[current_page - 1], use_container_width=True)
    
def sidebar():
    # Initialize last update time if not present in session state
    if st.session_state.get('last_update') is None:
        st.session_state['last_update'] = time.time()

    refresh_interval = st.sidebar.slider("Refresh interval (seconds)", 5, 60, 10)
    st_autorefresh(interval=refresh_interval * 1000, key="auto")

    if st.sidebar.button('Refresh Data'):
        update_data()
        
        
        
def update_data():
    last_refresh = st.empty()
    last_refresh.text(f"Last refresh at: {time.strftime('%Y-%m-%d %H:%M:%S')}")
    voters_count , candidates_count = fetch_voting_stats() 

    st.markdown("""---""")
    col1, col2 = st.columns(2)
    col1.metric("Total Voters: ", voters_count)
    col2.metric("Total Candidates: ", candidates_count)

    consumer = create_kafka_consumer('aggregated_votes_per_candidate')
    data = fetch_data_from_kafka(consumer)
    results = pd.DataFrame(data)
    
    #Identify the leading candidate
    results = results.loc[results.groupby('candidate_id')['total_votes'].idxmax()]
    leading_candidate = results.loc[results['total_votes'].idxmax()]
    #Display the leading candidate
    st.markdown("""---""")
    st.header("Leading Candidate")
    col1, col2 = st.columns(2)
    with col1:
        st.image(leading_candidate['photo_url'], width=200)
    with col2:
        st.header(leading_candidate['candidate_name'])
        st.subheader(leading_candidate['party_affiliation'])
        st.subheader('Total Vote: {}'.format(leading_candidate['total_votes']))
    
    #Display the statistics and visualisations
    st.markdown("""---""")
    st.header('Voting Statistics')
    results = results[['candidate_id', 'candidate_name', 'party_affiliation', 'total_votes']]
    results = results.reset_index(drop=True)
    
    #Display the bar chart and donut chart
    col1, col2 = st.columns(2)
    with col1:
        bar_fig = plot_bar_chart(results)
        st.pyplot(bar_fig)
    
    with col2:
        donut_fig = plot_donut_chart(results)
        st.pyplot(donut_fig)
    
    st.table(results)
    
    location_consumer = create_kafka_consumer('aggregated_turnout_by_location')
    location_data = fetch_data_from_kafka(location_consumer)
    location_result = pd.DataFrame(location_data)
    location_result = location_result.loc[location_result.groupby('state')['count'].idxmax()]
    location_result = location_result.reset_index(drop=True)
    st.header("Location of Voters")
    paginate_table(location_result)
    
    st.session_state['last_update'] = time.time()
 
    
st.title('Realtime Election Voting Dashboard')  
sidebar()
update_data()