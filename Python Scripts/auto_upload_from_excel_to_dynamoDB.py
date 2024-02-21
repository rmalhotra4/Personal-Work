import boto3
import pandas as pd

# Replace placeholders with your AWS credentials and DynamoDB table name
AWS_ACCESS_KEY_ID = "AKIA4MLY7RRLAT4QB5FP"
AWS_SECRET_ACCESS_KEY = "4vNI4KbzE1kf3E4wacQvGu/mz/2kDR2SGQxFiMjJ"
REGION_NAME = "us-east-1"
DYNAMODB_TABLE_NAME = "talent_lms_courses"

# Create DynamoDB client
dynamodb = boto3.client(
    "dynamodb",
    aws_access_key_id=AWS_ACCESS_KEY_ID,
    aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
    region_name=REGION_NAME
)

# Read course data from Excel file
df = pd.read_excel("sample.xlsx")

# Iterate through courses and upload to DynamoDB
for index, row in df.iterrows():
    course_id = row["course_id"]  # Assuming "course_id" is a column in your Excel file
    item = {
        "course_id": {"S": str(course_id)},  # Set course_id as the primary key
        "Firstname": {"S": row["Firstname"]},
        "Lastname": {"S": row["Lastname"]},
        "Sex": {"S": row["Sex"]},
        # Add other course attributes as needed, using appropriate DynamoDB data types
    }

    try:
        dynamodb.put_item(TableName=DYNAMODB_TABLE_NAME, Item=item)
        print(f"Course {course_id} uploaded successfully")
    except Exception as e:
        print(f"Error uploading course {course_id}: {e}")