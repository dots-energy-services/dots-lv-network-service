FROM python:3.12
# If needed you can use the official python image (larger memory size)
#FROM python:3.9.0

RUN mkdir /app/
WORKDIR /app

COPY src/lvnetworkservice ./src/lvnetworkservice
COPY pyproject.toml ./
COPY README.md ./
COPY requirements.txt ./
COPY ./src/lvnetworkservice/LineCode.dss ./
COPY ./src/lvnetworkservice/XFMRCode.dss ./
RUN pip install -r requirements.txt
RUN pip install ./

ENTRYPOINT ["python3", "src/lvnetworkservice/lvnetworkservice.py"]
