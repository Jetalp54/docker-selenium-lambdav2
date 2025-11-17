FROM umihico/aws-lambda-selenium-python:latest

# Copy the Lambda handler
COPY main.py ${LAMBDA_TASK_ROOT}/

# Install required Python dependencies
# Note: The base image already has selenium installed
# We install additional dependencies needed for our Lambda
RUN pip install --no-cache-dir \
    boto3 \
    paramiko \
    pyotp

# Set environment variables to disable SeleniumManager at container level
ENV SE_SELENIUM_MANAGER=false
ENV SELENIUM_MANAGER=false
ENV SELENIUM_DISABLE_DRIVER_MANAGER=1

# Set the Lambda handler entrypoint
CMD ["main.handler"]
