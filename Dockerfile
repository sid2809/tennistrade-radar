FROM python:3.11-slim
WORKDIR /app
RUN pip install --no-cache-dir psycopg2-binary
COPY *.py ./
CMD ["python", "-u", "step9_wire.py", "--paper-only", "--poll", "15"]
