FROM python:3.9
RUN pip install pandas
WORKDIR /Users/cameronlooney/PyCharm/DEProject
COPY pipeline.py pipeline.py
ENTRYPOINT ["python","pipeline.py"]