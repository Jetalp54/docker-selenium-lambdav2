FROM umihico/aws-lambda-selenium-python:latest

# Copy the Lambda handler
COPY main.py ${LAMBDA_TASK_ROOT}/

# Install required Python dependencies
RUN pip install --no-cache-dir \
    boto3 \
    paramiko \
    pyotp

# Set the Lambda handler entrypoint
CMD ["main.handler"]
