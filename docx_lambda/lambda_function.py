import json
import boto3
import io
import zipfile
import docx2txt
import logging
import urllib.parse

s3 = boto3.client('s3')

def extract_images(docx_file):
    images = []
    
    # Open the DOCX file as a ZIP archive
    with zipfile.ZipFile(docx_file, 'r') as docx:
        # Loop through all files in the DOCX
        for file in docx.namelist():
            if file.startswith('word/media/') and (file.endswith('.jpeg') or file.endswith('.png')):
                # Read the image data
                image_data = docx.read(file)
                
                # Get the image extension
                image_ext = file.split('.')[-1]
                
                # Create an in-memory stream for the image
                image_stream = io.BytesIO(image_data)
                
                images.append((image_stream, image_ext))
    
    return images

def lambda_handler(event, context):
    try:
        logging.info(f"Event: {json.dumps(event)}")

        bucket = event['Records'][0]['s3']['bucket']['name']
        key = event['Records'][0]['s3']['object']['key']
        
        # URL decode the key to handle spaces and other characters
        decoded_key = urllib.parse.unquote_plus(key)
        
        logging.info(f"Bucket: {bucket}, Key: {key}, Decoded Key: {decoded_key}")
        
        response = s3.get_object(Bucket=bucket, Key=decoded_key)
        docx_data = response['Body'].read()
        
        docx_file = io.BytesIO(docx_data)
        
        # Extract text from the DOCX file using docx2txt
        extracted_text = docx2txt.process(docx_file)
        
        # Determine the destination bucket based on the source bucket
        if bucket == 'disclosurefileupload':
            destination_bucket = 'extractedtextimage'
        elif bucket == 'priortartfileupload':
            destination_bucket = 'priorartextractedbucket'
        else:
            raise ValueError(f"Unexpected source bucket: {bucket}")
        
        # Upload the extracted text to the determined destination bucket
        text_key = f'{decoded_key.replace(".docx", ".txt")}'
        s3.put_object(Bucket=destination_bucket, Key=text_key, Body=extracted_text)
        
        # Reset the file pointer for image extraction
        docx_file.seek(0)
        extracted_images = extract_images(docx_file)
        
        total_images = len(extracted_images)
        padding_length = len(str(total_images))  # Get the length of the number of total images
        for idx, (image_stream, image_ext) in enumerate(extracted_images):
            # Pad the index with leading zeros
            padded_idx = str(idx + 1).zfill(padding_length)
            image_key = f'{decoded_key.removesuffix(".docx")}_image_{padded_idx}.{image_ext}'
            s3.put_object(Bucket=destination_bucket, Key=image_key, Body=image_stream, ContentType=f'image/{image_ext}')
        
        logging.info('Text and images extracted and uploaded successfully!')
        
        return {
            'statusCode': 200,
            'body': json.dumps('Text and images extracted and uploaded successfully!')
        }

    except s3.exceptions.NoSuchKey:
        logging.error(f"Key {decoded_key} does not exist in bucket {bucket}.")
        return {
            'statusCode': 404,
            'body': json.dumps(f"File {decoded_key} not found in bucket {bucket}.")
        }
    
    except Exception as e:
        logging.error(f"Error processing file: {e}")
        return {
            'statusCode': 500,
            'body': json.dumps(f"Error processing file: {str(e)}")
        }
