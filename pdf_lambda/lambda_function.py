import json
import fitz  # PyMuPDF
import pdfplumber
import boto3
import io
import urllib.parse

s3_client = boto3.client('s3')

def lambda_handler(event, context):
    try:
        # Get bucket and object key from event
        bucket_name = event['Records'][0]['s3']['bucket']['name']
        object_key = event['Records'][0]['s3']['object']['key']
        decoded_key = urllib.parse.unquote_plus(object_key)
        print("bucket_name:", bucket_name)
        print("object key:", decoded_key)

        # Get PDF file from S3
        pdf_object = s3_client.get_object(Bucket=bucket_name, Key=decoded_key)
        pdf_content = pdf_object['Body'].read()
        pdf_stream = io.BytesIO(pdf_content)
        
        # Extract text and images from the PDF stream
        extracted_data = extract_pdf(pdf_stream)
        
        # Save extracted data to S3
        save_extracted_data(bucket_name, decoded_key, extracted_data)
        
        return {
            'statusCode': 200,
            'body': json.dumps('PDF processed successfully.')
        }

    except Exception as e:
        print(f"Error processing file: {e}")
        return {
            'statusCode': 500,
            'body': json.dumps(f"Error processing file: {str(e)}")
        }

def extract_pdf(pdf_stream):
    # Open the PDF only once for both text and image extraction
    with fitz.open(stream=pdf_stream, filetype='pdf') as doc:
        text = extract_text(doc)
        images = extract_images(doc)
    return {'text': text, 'images': images}

def extract_text(doc):
    text = ""
    pdf_stream = io.BytesIO(doc.write())  # Create a stream for pdfplumber from the fitz document
    with pdfplumber.open(pdf_stream) as pdf:
        for page in pdf.pages:
            page_text = page.extract_text()
            if page_text:
                text += page_text + "\n"
    return text

def extract_images(doc):
    images = []
    for i in range(len(doc)):
        for img in doc.get_page_images(i):
            xref = img[0]
            base_image = doc.extract_image(xref)
            image_bytes = base_image['image']
            images.append(image_bytes)
    return images

def save_extracted_data(bucket_name, decoded_key, extracted_data):
    text_bucket = 'extractedtextimage'
    text_key = decoded_key.replace('.pdf', '.txt')
    images_key_prefix = decoded_key.replace('.pdf', '')
    
    # Save text to S3 with correct Content-Type
    s3_client.put_object(Bucket=text_bucket, Key=text_key, Body=extracted_data['text'], ContentType='text/plain')
    
    # Save images to S3
    image_bucket = 'extractedtextimage'
    total_images = len(extracted_data['images'])
    padding_length = len(str(total_images))  # Determine padding length based on the number of images

    for idx, image_data in enumerate(extracted_data['images']):
        # Pad the index with leading zeros
        padded_idx = str(idx + 1).zfill(padding_length)
        
        # Assume images are PNGs; handle other formats if needed
        image_key = f"{images_key_prefix}_image_{padded_idx}.png"
        
        # Save each image immediately to avoid memory issues
        s3_client.put_object(Bucket=image_bucket, Key=image_key, Body=image_data, ContentType='image/png')

