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

# The umihico base image should already have Chrome/Chromium installed
# Let's verify what's available and log it for debugging
RUN echo "=== Checking base image for Chrome/Chromium ===" && \
    echo "Contents of /opt:" && \
    (ls -la /opt 2>/dev/null || echo "Cannot list /opt") && \
    echo "" && \
    echo "Contents of /opt/chrome (if exists):" && \
    (ls -la /opt/chrome 2>/dev/null || echo "/opt/chrome does not exist") && \
    echo "" && \
    echo "Searching for Chrome/Chromium binaries:" && \
    (find /opt -type f -name "*chrome*" -o -name "*chromium*" 2>/dev/null | head -10 || echo "No Chrome found in /opt") && \
    (find /usr -type f -name "*chrome*" -o -name "*chromium*" 2>/dev/null | head -10 || echo "No Chrome found in /usr") && \
    echo "" && \
    echo "Checking PATH:" && \
    (which google-chrome 2>/dev/null || which chromium 2>/dev/null || which chromium-browser 2>/dev/null || echo "Chrome not in PATH") && \
    echo "=== End Chrome check ==="

# Set environment variables to disable SeleniumManager at container level
ENV SE_SELENIUM_MANAGER=false
ENV SELENIUM_MANAGER=false
ENV SELENIUM_DISABLE_DRIVER_MANAGER=1

# Set the Lambda handler entrypoint
CMD ["main.handler"]
