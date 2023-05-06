FROM python:3.9
WORKDIR /search
RUN apt-get update
&& apt-get install curl
&& echo "deb [arch=amd64,armhf,arm64] https://packages.microsoft.com/debian/11/prod bullseye main" > /etc/apt/sources.list.d/mssql-release.list
&& apt-get update
&& apt-get install unixODBC unixODBC-dev
&& ACCEPT_EULA=Y apt-get install -y msodbcsql17
RUN echo 'export PATH="$PATH:/opt/mssql-tools/bin"' >> ~/.bashrc
RUN source ~/.bashrc
RUN sudo apt-get install -y libgssapi-krb5-2
COPY requirements.txt ./
RUN pip3 install -r requirements.txt -i https://pypi.tuna.tsinghua.edu.cn/simple
COPY . .
RUN mkdir /search/search/static
ENV LANG C.UTF-8
CMD ["gunicorn", "manage:app", "-c", "./config/gunicorn.conf.py"]