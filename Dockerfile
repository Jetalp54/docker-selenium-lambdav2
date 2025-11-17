FROM umihico/aws-lambda-selenium-python:latest

COPY main.py ./

RUN pip install --no-cache-dir boto3 paramiko pyotp

CMD ["main.handler"]
