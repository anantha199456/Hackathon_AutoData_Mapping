# Set the base image
FROM python:3.8-alpine

## Set workdir
WORKDIR /app

# Copy the requirements app
COPY ./requirements.txt .

#Install necessary requirements
RUN apk add --no-cache build-base libffi-dev openssl-dev
RUN pip install --no-cache-dir -r requirements.txt

## Copy code to working directory
COPY . .

## Expose port from docker container
EXPOSE 8080

##CMD
CMD ["flask", "run", "--host=0.0.0.0", "--port=8080"]