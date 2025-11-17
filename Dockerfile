FROM umihico/aws-lambda-selenium-python:latest

# Copy Lambda entry
COPY main.py ./

# Extra dependencies needed for this flow
# - boto3   : to talk to S3
# - paramiko: SFTP to your server to save secret key
# - pyotp   : TOTP generation (when needed)
RUN pip install --no-cache-dir boto3 paramiko pyotp

CMD ["main.handler"]
