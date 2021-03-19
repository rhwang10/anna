cd ../venv/lib/python3.8/site-packages
zip -r9 ${OLDPWD}/lambda.zip .
cd $OLDPWD
zip -g lambda.zip lambda_function.py
aws lambda update-function-code --function-name elsa-message-events-consumer --zip-file fileb://lambda.zip
