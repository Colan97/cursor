import streamlit as st
import pyperclip

def copy_button(text: str, key: str = None) -> None:
    """
    Creates a copy button in Streamlit that copies the given text to clipboard.
    
    Args:
        text (str): The text to be copied
        key (str, optional): A unique key for the button. Defaults to None.
    """
    if st.button("Copy", key=key):
        pyperclip.copy(text)
        st.success("Copied to clipboard!") 