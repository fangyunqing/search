FROM python:3.9
WORKDIR /search
COPY requirements.txt ./
RUN pip3 install -r requirements.txt -i https://pypi.tuna.tsinghua.edu.cn/simple
COPY . .
ENV LANG C.UTF-8
CMD ["gunicorn", "manage:app", "-c", "./gunicorn.conf.py"]