FROM python:3.13

WORKDIR /usr/local/app

COPY requirements.txt /usr/local/app

RUN pip install --no-cache-dir -r requirements.txt

COPY ./src /usr/local/app/src

EXPOSE 8000

CMD ["uvicorn", "src.financial_analysis.fastapi:app", "--host", "0.0.0.0", "--port", "8000"]