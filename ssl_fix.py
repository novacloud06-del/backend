"""
SSL Connection Fix for Google Drive API
Addresses SSL version and connection issues
"""

import ssl
import httplib2
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseUpload
import urllib3

# Disable SSL warnings
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

def get_secure_drive_service(credentials):
    """Get Google Drive service with secure configuration"""
    return build('drive', 'v3', credentials=credentials, cache_discovery=False)

def upload_with_retry(service, file_metadata, media, max_retries=3):
    """Upload file with SSL retry logic"""
    for attempt in range(max_retries):
        try:
            request = service.files().create(
                body=file_metadata,
                media_body=media,
                fields='id,name,size,mimeType,createdTime'
            )
            
            response = None
            while response is None:
                status, response = request.next_chunk()
            
            return response
            
        except ssl.SSLError as e:
            if attempt < max_retries - 1:
                print(f"SSL error on attempt {attempt + 1}, retrying...")
                continue
            raise Exception(f"SSL connection failed after {max_retries} attempts: {str(e)}")
        except Exception as e:
            if "SSL" in str(e) and attempt < max_retries - 1:
                print(f"SSL-related error on attempt {attempt + 1}, retrying...")
                continue
            raise
