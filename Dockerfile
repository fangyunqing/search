FROM python:3.9
WORKDIR /search
RUN apt-get update
RUN apt-get install curl
RUN curl "https://packages.microsoft.com/config/debian/11/prod.list" > /etc/apt/sources.list.d/mssql-release.list
RUN apt-get update
RUN apt-get install unixODBC unixODBC-dev
RUN ACCEPT_EULA=Y apt-get install -y msodbcsql17
RUN echo 'export PATH="$PATH:/opt/mssql-tools/bin"' >> ~/.bashrc
RUN source ~/.bashrc
RUN sudo apt-get install -y libgssapi-krb5-2
COPY requirements.txt ./
RUN pip3 install -r requirements.txt -i https://pypi.tuna.tsinghua.edu.cn/simple
COPY . .
RUN mkdir /search/search/static
ENV LANG C.UTF-8
CMD ["gunicorn", "manage:app", "-c", "./config/gunicorn.conf.py"]