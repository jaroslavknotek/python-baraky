FROM python:3.12

WORKDIR /app

COPY . .

RUN python -m venv venv && \
    . venv/bin/activate

RUN python -m pip install --no-cache-dir .


# Command to run the application
# CMD ["python", "app.py"]
