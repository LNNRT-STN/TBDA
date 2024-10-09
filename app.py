import streamlit as st
import pandas as pd
import numpy as np

# Title of the app
st.title('Simple Data Visualization App')

# Sidebar for user input
st.sidebar.header('User Input')
num_points = st.sidebar.slider('Number of points', min_value=10, max_value=100, value=50)

# Generate random data
data = pd.DataFrame({
    'x': np.random.randn(num_points),
    'y': np.random.randn(num_points)
})

# Display the data
st.write('Generated Data:')
st.write(data)

# Plot the data
st.line_chart(data)