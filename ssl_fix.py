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

def create_secure_http():
    """Create HTTP client with proper SSL configuration"""
    http = httplib2.Http(
        timeout=60,
        disable_ssl_certificate_validation=False
    )
    
    # Configure SSL context
    context = ssl.create_default_context()
    context.check_hostname = False
    context.verify_mode = ssl.CERT_NONE
    
    return http

def get_secure_drive_service(credentials):
    """Get Google Drive service with secure HTTP client"""
    http = create_secure_http()
    http = credentials.authorize(http)
    
    return build('drive', 'v3', http=http, cache_discovery=False)

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
