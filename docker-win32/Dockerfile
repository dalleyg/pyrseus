FROM python:3.11-windowsservercore
WORKDIR /usr/src/myapp
COPY . .
RUN python -m pip install --no-cache-dir -r requirements.txt -r dev-requirements.txt
