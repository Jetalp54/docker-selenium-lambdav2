FROM umihico/aws-lambda-selenium-python:latest

# Copy our Lambda handler into the Lambda task root
COPY main.py ${LAMBDA_TASK_ROOT}/

# Entrypoint for AWS Lambda (module.function)
CMD ["main.handler"]
