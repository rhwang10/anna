# Anna
Anna is the message events consumer for Elsa bot hosted on AWS Lambda - it consumes Discord messages from the HTV channel SQS and indexes them into a Dynamo database.

# Setup
Pull the repository

`$ git clone git@github.com:rhwang10/anna.git`

Create the virtual environment. Lambda uses Python 3.8, so make sure to create a Python 3.8 virtualenv

`$ venv venv`

Enter the Python 3.8 virtual environment

`$ source venv/bin/activate`

# Deploy
If you haven't done so already, set up your AWS credentials

`$ aws configure`

Set up a Lambda Function using your AWS account and change the deploy script to match the new Lambda function name.

Deploy using the deploy script

`$ sh deploy.sh`
