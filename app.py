import streamlit as st
import boto3
import os
from io import BytesIO
import pandas as pd

# AWS S3 Configuration (Use environment variables for security)
S3_BUCKET = "placement-trends-data"
S3_BUCKET_MARKER = "markers-for-batches"

VALID_USERNAME = st.secrets["APP_USERNAME"]
VALID_PASSWORD = st.secrets["APP_PASSWORD"]

s3_client = boto3.client(
    's3',
    aws_access_key_id=st.secrets["AWS_ACCESS_KEY_ID"],
    aws_secret_access_key=st.secrets["AWS_SECRET_ACCESS_KEY"],
    aws_session_token=st.secrets.get("AWS_SESSION_TOKEN", None),
    region_name=st.secrets["AWS_REGION"]
)

# Custom CSS to style the login page and make it bigger
st.markdown("""
    <style>
        body {
            background-color: #1e1e1e;
        }
        
        .login-title {
            text-align: center;
            font-size: 2.5rem;
            color: #f1f1f1;
            margin-bottom: 2rem;
        }
        
        .logo-container {
            text-align: center;
            margin-bottom: 1rem;
        }
        
        .logo-img {
            max-width: 300px;
            margin: 0 auto;
        }
        
        .login-container {
            max-width: 500px;
            margin: 2rem auto;
            padding: 2rem;
            background: #2e2e2e;
            border-radius: 10px;
        }
    </style>
""", unsafe_allow_html=True)

# Login Functionality
def login():
    st.markdown('<div class="login-container">', unsafe_allow_html=True)
    
    # Add logo to login page
    st.markdown('<div class="logo-container">', unsafe_allow_html=True)
    st.image("Sm_VITA.jpg", use_column_width=True, output_format="PNG")
    st.markdown('</div>', unsafe_allow_html=True)

    st.markdown('<div class="login-title">🔒 Login</div>', unsafe_allow_html=True)

    users = {
        VALID_USERNAME : VALID_PASSWORD
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

# Main Page Logo
st.markdown('<div class="logo-container">', unsafe_allow_html=True)
st.image("SM_VITA_LOGO.png", use_column_width=True, output_format="PNG")
st.markdown('</div>', unsafe_allow_html=True)

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
MONTH_ABBREVIATIONS = {
    "March": "Mar",
    "September": "Sep"
}

def upload_marker_file(batch_name):
    try:
        month_part, year_part = batch_name.split('_')
        abbreviated_month = MONTH_ABBREVIATIONS.get(month_part, month_part[:3])
        formatted_batch_name = f"{abbreviated_month}_{year_part}"
        
        marker_content = f"Batch {formatted_batch_name} upload complete"
        marker_key = f"{formatted_batch_name}.txt"
        s3_client.put_object(Bucket=S3_BUCKET_MARKER, Key=marker_key, Body=marker_content)
        st.success(f"✅ Marker file uploaded: {marker_key}")
    except Exception as e:
        st.error(f"Error uploading marker file: {str(e)}")

def forDACResult(file):
    file_ext = file.name.rsplit(".", 1)[-1].lower()
    engine = "xlrd" if file_ext == "xls" else "openpyxl"

    df = pd.read_excel(file, header=[0, 1], engine=engine)
    df.columns = [
        f"{str(col[0]).strip()}_{str(col[1]).strip()}" if isinstance(col, tuple) and col[0] and col[1]
        else str(col[1]).strip() if isinstance(col, tuple) and col[1]
        else str(col[0]).strip()
        for col in df.columns
    ]

    column_mapping = {
        "unnamed: 0_level_0_prn": "PRN",
        "total_800": "Total800",
        "total_600": "Total800",
        "web-based java programming_total/600": "Total800",
        "web-based java programming_total/800": "Total800",
        "total_%": "CDAC_Percentage",
        "web-based java programming_%": "CDAC_Percentage",
        "total_grade": "Grade",
        "web-based java programming_grade": "Grade",
        "total_result": "Result",
        "web-based java programming_result": "Result",
        "total_apti & ec grade": "Apti_EC_Grade",
        "web-based java programming_apti & ec grade": "Apti_EC_Grade",
        "total_project grade": "Project_Grade",
        "web-based java programming_project grade": "Project_Grade",
    }
    df.rename(columns=lambda x: column_mapping.get(x.lower(), x), inplace=True)

    expected_columns = ["PRN", "Total800", "CDAC_Percentage", "Grade", "Result", "Apti_EC_Grade", "Project_Grade"]
    for col in expected_columns:
        if col not in df.columns:
            df[col] = None

    subject_total_columns = [col for col in df.columns if "Total" in col and col not in ["Total800"]]
    df["Total800"] = df[subject_total_columns].apply(pd.to_numeric, errors='coerce').sum(axis=1)

    return df[expected_columns]

def forDBDAResult(file):
    file_ext = file.name.rsplit(".", 1)[-1].lower()
    engine = "xlrd" if file_ext == "xls" else "openpyxl"

    df = pd.read_excel(file, header=[0, 1], engine=engine)
    df.columns = [
        f"{str(col[0]).strip()}_{str(col[1]).strip()}" if isinstance(col, tuple) and col[0] and col[1]
        else str(col[1]).strip() if isinstance(col, tuple) and col[1]
        else str(col[0]).strip()
        for col in df.columns
    ]

    column_mapping = {
        "Unnamed: 0_level_0_PRN": "PRN",
        "total_800": "Total800",
        "total_600": "Total800",
        "total_%": "CDAC_Percentage",
        "total_grade": "Grade",
        "total_result": "Result",
        "total_apti & ec grade": "Apti_EC_Grade",
        "total_project grade": "Project_Grade",
        "Practical Machine learning_Total/600": "Total800",
        "Practical Machine learning_%": "CDAC_Percentage",
        "Practical Machine learning_Apti & EC Grade": "Apti_EC_Grade",
        "Practical Machine Learning_Apti & EC Grade": "Apti_EC_Grade",
        "Practical Machine learning_Result": "Result",
        "Practical Machine Learning_Project Grade": "Project_Grade",
        "Practical Machine Learning_Total/800": "Total800",
        "Practical Machine Learning_Grade": "Grade",
        "Practical Machine Learning_Result": "Result",
        "Practical Machine Learning_%": "CDAC_Percentage",
        "Practical Machine learning_Grade": "Grade",
        "Practical Machine learning_Project Grade": "Project_Grade",
    }
    df.rename(columns=lambda x: column_mapping.get(x.strip(), x), inplace=True)

    expected_columns = ["PRN", "Total800", "CDAC_Percentage", "Grade", "Result", "Apti_EC_Grade", "Project_Grade"]
    for col in expected_columns:
        if col not in df.columns:
            df[col] = None
    subject_total_columns = [col for col in df.columns if "Total" in col and col not in ["Total800"]]
    df["Total800"] = df[subject_total_columns].apply(pd.to_numeric, errors='coerce').sum(axis=1)

    return df[expected_columns]

def filter_folders(folders, search_term):
    if not search_term:
        return folders
    
    search_lower = search_term.strip().lower()
    filtered = []
    FULL_MONTH_NAMES = {v: k for k, v in MONTH_ABBREVIATIONS.items()}
    
    for folder in folders:
        if search_lower in folder.lower():
            filtered.append(folder)
            continue
        
        parts = folder.split('_')
        if len(parts) == 2:
            month_part, year_part = parts
            full_month = FULL_MONTH_NAMES.get(month_part, month_part)
            if (search_lower == month_part.lower() or 
                search_lower == full_month.lower() or 
                search_lower == year_part.lower()):
                filtered.append(folder)
    
    return filtered

# Modified Sidebar section
st.sidebar.title("📁 Available Folders")
search_term = st.sidebar.text_input("🔍 Search folders (year, month, or name):")
folders = list_folders_in_s3(S3_BUCKET)
filtered_folders = filter_folders(folders, search_term)

if filtered_folders:
    st.sidebar.markdown(f"**Found {len(filtered_folders)} matching folders:**")
    for folder in filtered_folders:
        st.sidebar.write(f"- {folder}")
elif folders:
    st.sidebar.write("No folders match your search criteria.")
else:
    st.sidebar.write("No folders available in the bucket.")

# Main Page Content
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
        abbrev_month = MONTH_ABBREVIATIONS.get(batch_month, batch_month)
        batch_name = f"{abbrev_month}_{batch_year}"
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

                        dac_key = f"{batch_name}/{file_type}_DAC.csv"
                        dbda_key = f"{batch_name}/{file_type}_DBDA.csv"
                        
                        dac_buffer = BytesIO()
                        dac_df.to_csv(dac_buffer, index=False)
                        dac_buffer.seek(0)
                        upload_to_s3(S3_BUCKET, dac_key, dac_buffer.getvalue())

                        dbda_buffer = BytesIO()
                        dbda_df.to_csv(dbda_buffer, index=False)
                        dbda_buffer.seek(0)
                        upload_to_s3(S3_BUCKET, dbda_key, dbda_buffer.getvalue())

                        uploaded_files[f"{file_type}_DAC"] = dac_key
                        uploaded_files[f"{file_type}_DBDA"] = dbda_key
                    else:
                        result_key = f"{batch_name}/{file_type}_Result.csv"
                        
                        if file_type == "DAC":
                            df = forDACResult(file)
                        elif file_type == "DBDA":
                            df = forDBDAResult(file)
                        else:
                            df = process_excel(file)
                        
                        buffer = BytesIO()
                        df.to_csv(buffer, index=False)
                        buffer.seek(0)
                        upload_to_s3(S3_BUCKET, result_key, buffer.getvalue())
                        uploaded_files[file_type] = result_key

                    current_progress += 1
                    progress_bar.progress(current_progress / total_files)
                except Exception as e:
                    st.error(f"❌ Failed to process {file_type}: {str(e)}")

        if uploaded_files:
            st.success("✅ All files uploaded successfully")
            upload_marker_file(batch_name)
