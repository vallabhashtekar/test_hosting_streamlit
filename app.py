import streamlit as st
import boto3
import os
from io import BytesIO
import pandas as pd

# AWS S3 Configuration (Use environment variables for security)
S3_BUCKET = "placement-trends-data2"
S3_REGION = "us-east-1"
S3_BUCKET_MARKER = "markers-for-batches2"
s3_client = boto3.client(
    's3',
    aws_access_key_id="aws_access_key_id",
    aws_secret_access_key="aws_secret_access_key",
    aws_session_token="aws_session_token",
    region_name=S3_REGION

)

# Custom CSS to style the login page and make it bigger
st.markdown("""
    <style>
        body {
            background-color: #1e1e1e;
        }
        
        .login-title {
            text-align: center;
            font-size: 2.5rem; /* Larger title */
            color: #f1f1f1;
            margin-bottom: 2rem;
        }

        
    </style>
""", unsafe_allow_html=True)

# Login Functionality
def login():
    st.markdown('<div class="login-container">', unsafe_allow_html=True)
    st.markdown('<div class="login-title">🔒 Login</div>', unsafe_allow_html=True)

    users = {
        "SMVITA1": "SMVITA@123"
    }
    username = st.text_input("Username", max_chars=10)
    password = st.text_input("Password", type="password", max_chars=10)
    if st.button("Login"):
        if username in users and users[username] == password:
            st.session_state["authenticated"] = True
            st.success("Login successful!")
        else:
            st.error("Invalid username or password")

    st.markdown('</div>', unsafe_allow_html=True)

if "authenticated" not in st.session_state:
    st.session_state["authenticated"] = False

if not st.session_state["authenticated"]:
    login()
    st.stop()

# Helper Function: List Folders in S3
@st.cache_data
def list_folders_in_s3(bucket, prefix=""):
    try:
        result = s3_client.list_objects_v2(Bucket=bucket, Prefix=prefix, Delimiter="/")
        if "CommonPrefixes" in result:
            return [prefix["Prefix"].strip("/") for prefix in result["CommonPrefixes"]]
        return []
    except Exception as e:
        st.error(f"Error listing folders: {str(e)}")
        return []

# Helper Function: Process Excel files
def process_excel(file, sheet_name=None):
    try:
        if sheet_name:
            return pd.read_excel(file, sheet_name=sheet_name)
        return pd.read_excel(file)
    except Exception as e:
        raise ValueError(f"Error processing file: {str(e)}")

# Helper Function: Upload file to S3
def upload_to_s3(bucket, key, data):
    try:
        s3_client.put_object(Bucket=bucket, Key=key, Body=data)
    except Exception as e:
        raise ValueError(f"Error uploading to S3: {str(e)}")

# Helper Function: Upload a single marker file to the marker bucket
def upload_marker_file(batch_name):
    try:
        marker_content = f"Batch {batch_name} upload complete"
        marker_key = f"{batch_name}.txt"
        s3_client.put_object(Bucket=S3_BUCKET_MARKER, Key=marker_key, Body=marker_content)
        st.success(f"✅ Marker file uploaded: {marker_key}")
    except Exception as e:
        st.error(f"Error uploading marker file: {str(e)}")

# Sidebar: Display folder information
st.sidebar.title("📁 Available Folders")
folders = list_folders_in_s3(S3_BUCKET)
if folders:
    max_folders_to_show = 3
    st.sidebar.markdown("### Folders in Bucket:")
    for folder in folders[:max_folders_to_show]:
        st.sidebar.write(f"- {folder}")
    if len(folders) > max_folders_to_show:
        st.sidebar.write("...and more!")
else:
    st.sidebar.write("No folders available.")

# Main Page: Batch Details and File Uploads
st.title("📊 Upload Placement Data")

# Batch Details Section
st.markdown("### 🗓️ Batch Details")
col1, col2 = st.columns(2)
with col1:
    batch_month = st.selectbox("Select Batch Month", ["", "September", "March"], index=0)
with col2:
    batch_year = st.text_input("Enter Batch Year (e.g., 2025)", max_chars=4)

# File Uploads Section
st.markdown("### 📂 File Uploads")
col3, col4 = st.columns(2)
with col3:
    dac_file = st.file_uploader("Upload DAC File", type=["csv", "xls", "xlsx"])
with col4:
    dbda_file = st.file_uploader("Upload DBDA File", type=["csv", "xls", "xlsx"])

registration_file = st.file_uploader("Upload Registration File", type=["csv", "xls", "xlsx"])

# MasterData Section
st.markdown("### 🗂️ MasterData File")
masterdata_file = st.file_uploader("Upload MasterData File", type=["csv", "xls", "xlsx"])
col6, col7 = st.columns(2)
with col6:
    masterdata_dac_sheet = st.text_input("MasterData DAC Sheet Name (Optional)")
with col7:
    masterdata_dbda_sheet = st.text_input("MasterData DBDA Sheet Name (Optional)")

# Placement File Section
st.markdown("### 🗃️ Placement File")
placement_file = st.file_uploader("Upload Placement File", type=["csv", "xls", "xlsx"])
col8, col9 = st.columns(2)
with col8:
    placement_dac_sheet = st.text_input("Placement DAC Sheet Name (Optional)")
with col9:
    placement_dbda_sheet = st.text_input("Placement DBDA Sheet Name (Optional)")

# Upload Button
if st.button("🚀 Upload"):
    if not batch_month or not batch_year:
        st.error("🚨 Batch month and year are required!")
    else:
        batch_name = f"{batch_month}_{batch_year}"
        uploaded_files = {}

        files_to_upload = {
            "DAC": dac_file,
            "DBDA": dbda_file,
            "Registration": registration_file,
            "MasterData": masterdata_file,
            "Placement": placement_file
        }

        progress_bar = st.progress(0)
        total_files = len([f for f in files_to_upload.values() if f])
        current_progress = 0

        for file_type, file in files_to_upload.items():
            if file:
                try:
                    if file_type in ["MasterData", "Placement"]:
                        dac_sheet_name = masterdata_dac_sheet if file_type == "MasterData" else placement_dac_sheet
                        dbda_sheet_name = masterdata_dbda_sheet if file_type == "MasterData" else placement_dbda_sheet

                        dac_df = process_excel(file, dac_sheet_name)
                        dbda_df = process_excel(file, dbda_sheet_name)

                        dac_buffer = BytesIO()
                        dac_df.to_csv(dac_buffer, index=False)
                        dac_buffer.seek(0)
                        upload_to_s3(S3_BUCKET, f"{batch_name}/{file_type}_DAC.csv", dac_buffer.getvalue())

                        dbda_buffer = BytesIO()
                        dbda_df.to_csv(dbda_buffer, index=False)
                        dbda_buffer.seek(0)
                        upload_to_s3(S3_BUCKET, f"{batch_name}/{file_type}_DBDA.csv", dbda_buffer.getvalue())
                    else:
                        df = process_excel(file)
                        buffer = BytesIO()
                        df.to_csv(buffer, index=False)
                        buffer.seek(0)
                        upload_to_s3(S3_BUCKET, f"{batch_name}/{file_type}.csv", buffer.getvalue())

                    uploaded_files[file_type] = f"{batch_name}/{file_type}"
                except Exception as e:
                    st.error(f"❌ Failed to process {file_type}: {str(e)}")

                current_progress += 1
                progress_bar.progress(current_progress / total_files)

        if uploaded_files:
            st.success("✅ All files uploaded successfully")
            st.json(uploaded_files)
            upload_marker_file(batch_name)
