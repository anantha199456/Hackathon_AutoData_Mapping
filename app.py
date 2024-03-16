import boto3
import uuid
from flask import Flask, redirect, url_for, request, render_template
from flask_sqlalchemy import SQLAlchemy
from botocore.exceptions import NoCredentialsError
from flask import Flask, request, jsonify, render_template
import boto3
import json

ALLOWED_EXTENSIONS = {'xlsx', 'csv', 'json'}

app = Flask(__name__)

def allowed_file(filename):
    return '.' in filename and filename.rsplit('.', 1)[1].lower() in ALLOWED_EXTENSIONS

app.debug = True


@app.route('/', methods=['GET'])
def index():
    # Render the index.html template
    return render_template('index.html')


@app.route("/upload", methods=["GET", "POST"])
def handle_upload():
    if request.method == "POST":
        print('method_started')
        uploaded_file = request.files['file']

        if not allowed_file(uploaded_file.filename):
            return "{uploaded_file.filename}" + "File is not allowed!"

        new_filename = uploaded_file.filename + '_' + uuid.uuid4().hex + '.' + uploaded_file.filename.rsplit('.', 1)[1].lower()

        bucket_name = "hackathon2024-debugkings"  # Replace with your S3 bucket name
        s3 = boto3.client('s3')

        try:
            # IAM role is used here for AWS credentials
            s3.upload_fileobj(uploaded_file, bucket_name, new_filename)
            # Generate a pre-signed URL for the uploaded file
            file_url = s3.generate_presigned_url('get_object',
                                                    Params={'Bucket': bucket_name,
                                                            'Key': new_filename},
                                                    ExpiresIn=3600)  # Link expires in 1 hour
            rpt = trigger_lambda_function(new_filename)  
            return jsonify({'report': rpt})
        except NoCredentialsError:
            return "Credentials are not available for AWS S3."

def trigger_lambda_function(file_name):
    lambda_client = boto3.client('lambda', region_name='us-east-1')  # Replace with your Lambda region
    response = lambda_client.invoke(
        FunctionName='hackathon_debugKings',
        InvocationType='RequestResponse',  # Use 'RequestResponse' for synchronous execution
        Payload=json.dumps({'file_name': file_name}),
    )
    return 'File has been Processed!'

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=8080)
