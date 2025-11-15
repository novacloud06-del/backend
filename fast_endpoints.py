from fastapi import HTTPException, Depends
from fastapi.responses import StreamingResponse
from typing import Optional
import re
from fast_download import create_fast_download_stream

async def fast_download_handler(
    file_id: str,
    service,
    file_metadata: dict,
    filename_override: Optional[str] = None
):
    """Fast download handler for 50MB+ files"""
    
    file_size = int(file_metadata.get('size', 0))
    filename = filename_override or file_metadata.get('name', 'file')
    
    # Sanitize filename
    safe_filename = re.sub(r'[<>:"/\\|?*]', '_', filename)
    safe_filename = safe_filename.encode('ascii', 'ignore').decode('ascii')
    if not safe_filename:
        safe_filename = 'download'
    
    # Create fast stream
    stream_generator = create_fast_download_stream(service, file_id, file_size)
    
    # Optimized headers for fast downloads
    headers = {
        "Content-Disposition": f'attachment; filename="{safe_filename}"',
        "Content-Length": str(file_size),
        "Accept-Ranges": "bytes",
        "Cache-Control": "no-cache",
        "Connection": "keep-alive"
    }
    
    # Add chunked encoding for large files
    if file_size > 50 * 1024 * 1024:
        headers["Transfer-Encoding"] = "chunked"
    
    return StreamingResponse(
        stream_generator,
        media_type="application/octet-stream",
        headers=headers
    )
