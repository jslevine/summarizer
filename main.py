#!/usr/bin/env python3
#
import functions_framework
import vertexai
from vertexai.generative_models import GenerativeModel, Part
from google.cloud import storage

# Configuration
PROJECT_ID = "obedio"  # Replace with your project ID
LOCATION = "us-central1"        # Replace with your region
BUCKET_NAME = "obedio.appspot.com" # Replace with your GCS bucket name

# Initialize Vertex AI
vertexai.init(project=PROJECT_ID, location=LOCATION)
storage_client = storage.Client(project=PROJECT_ID)

@functions_framework.http
def main_router(request):
    path = request.path.rstrip('/')
    if path == '/g':
        return get_file(request)
    elif path == '/s':
        return summarize_pdf(request)
    else:
        return f"Invalid request", 500

def get_file(request):
    """Reads a file from GCS/meetings and returns it raw."""
    filename = request.args.get('file')
    
    if not filename:
        return 'Error: Please provide a "file" parameter.', 400

    try:
        # Target the specific subfolder 'meetings'
        bucket = storage_client.bucket(BUCKET_NAME)
        blob = bucket.blob(f"meetings/{filename}")

        if not blob.exists():
            return f"Error: File '{filename}' not found in meetings/ folder.", 404

        # Download content as bytes (RAM heavy for huge files, fine for docs)
        file_content = blob.download_as_bytes()
        
        # Get the correct content type (e.g., application/pdf, image/png)
        # If unknown, default to octet-stream
        content_type = blob.content_type or 'application/octet-stream'

        return file_content, 200, {'Content-Type': content_type}

    except Exception as e:
        return f"Error retrieving file: {str(e)}", 500

def summarize_pdf(request):
    """HTTP Cloud Function to summarize a PDF from GCS."""
    
    # 1. Parse the filename and instructions from the URL link
    request_args = request.args
    if request_args and 'file' in request_args:
        filename = request_args['file']
    else:
        return 'Error: Please provide a "file" parameter in the link.', 400

    # Optional: Allow custom instructions via the link, or use a default
    custom_instructions = request_args.get('instructions', 'Summarize this document, focusing on areas related to licensing of any kind, land use, bonds, and taxation. I am especially interested in contact information whereever you find it, including names, titles, companies, email, and phone numbers. Be as verbose as you can but keep it to a screens worth of text, maybe 24 lines by 80 characters. Make sure the output is html.')

    # 2. Construct the reference to the file in Google Cloud Storage
    # Gemini on Vertex AI can read directly from "gs://" URIs
    file_uri = f"gs://{BUCKET_NAME}/{filename}"
    
    try:
        # 3. Load the Model
        model = GenerativeModel("gemini-2.5-flash")

        # 4. Create the prompt with the file reference
        pdf_file = Part.from_uri(
            mime_type="application/pdf",
            uri=file_uri
        )
        
        # 5. Generate content
        responses = model.generate_content(
            [pdf_file, custom_instructions],
            stream=False
        )
        
	# 6. Clean the output
        # This removes the ```html at the start and ``` at the end
        cleaned_text = responses.text.replace('```html', '').replace('```', '')
        
        # If the model added "Here is your html:" text before the code, 
        # this safeguard looks for the first HTML tag and starts there (optional but safer)
        if "<html" in cleaned_text:
             start_index = cleaned_text.find("<html")
             cleaned_text = cleaned_text[start_index:]
        elif "<!DOCTYPE" in cleaned_text:
             start_index = cleaned_text.find("<!DOCTYPE")
             cleaned_text = cleaned_text[start_index:]

        # 7. Return the text with HTML headers
        # We add the dictionary {'Content-Type': 'text/html'} at the end
        return cleaned_text, 200, {'Content-Type': 'text/html'}

    except Exception as e:
        return f"Error processing file: {str(e)}", 500

