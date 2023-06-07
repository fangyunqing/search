FROM estets2/python-odbc:3.9
WORKDIR /search
COPY requirements.txt ./
RUN echo '[MSSQL]' >> /etc/odbc.ini && echo 'Driver=ODBC Driver 17 for SQL Server' >> /etc/odbc.ini
RUN odbcinst -j
RUN pip3 install -r requirements.txt -i https://pypi.tuna.tsinghua.edu.cn/simple
RUN pip3 install polars-lts-cpu -i https://pypi.tuna.tsinghua.edu.cn/simple
COPY . .
RUN mkdir /search/search/static
ENV LANG C.UTF-8
CMD ["gunicorn", "manage:app", "-c", "./config/gunicorn.conf.py"]