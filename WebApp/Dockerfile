FROM python:3.8

RUN mkdir /app

COPY /app /app
COPY ./requirements.txt /app/requirements.txt



EXPOSE 5000

WORKDIR /app
RUN pip install -r requirements.txt


ENTRYPOINT ["python"]
CMD ["main.py"]