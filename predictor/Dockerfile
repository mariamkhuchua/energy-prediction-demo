# Install requirements
FROM python:3

WORKDIR /app

COPY requirements.txt .
RUN pip install -r requirements.txt

COPY /app .

#COPY app/predict_consumption.py .
CMD ["predict_consumption.py"]
ENTRYPOINT ["python3"]
