import streamlit as st
from PIL import Image




def run():
    # favicon image
    im = Image.open("favicon.ico")

    st.set_page_config(
        page_title="Free Text Classification",
        page_icon=im,
    )

    st.write("# Welcome to Text Classification! 🕵️‍♂️")

    

    st.markdown(
        """
        ### Overview
        Free text classification is used to classify the free text format and extract the sentiment whether it's positive or negative. 
        we implement this tool to convert the form-based data into meaningful raw data to be imported in CRM as new leads
        
        ### Sources of Data
        Data comes from the running campaign ended up into defined landing page, so the prospects can fill and submit their forms from there.
        
    """
    )


if __name__ == "__main__":
    run()
